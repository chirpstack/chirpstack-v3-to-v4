package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/brocaar/lorawan"
	"github.com/chirpstack/chirpstack-v3-to-v4/pbnew"
	"github.com/chirpstack/chirpstack-v3-to-v4/pbold"
	"github.com/go-redis/redis/v8"
	"github.com/gofrs/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/lib/pq/hstore"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

var (
	asConfigFile  string
	csConfigFile  string
	nsConfigFiles []string
	csSessionTTL  int
)

var (
	nsDB     *sqlx.DB
	asDB     *sqlx.DB
	csDB     *sqlx.DB
	nsRedis  redis.UniversalClient
	asRedis  redis.UniversalClient
	csRedis  redis.UniversalClient
	nsPrefix string
	asPrefix string
	csPrefix string
)

var rootCmd = &cobra.Command{
	Use:   "chirpstack-v3-to-v4",
	Short: "ChirpStack v3 to v4 migration utility",
	RunE:  run,
}

type Config struct {
	PostgreSQL struct {
		DSN string `mapstructure:"dsn"`
	} `mapstructure:"postgresql"`

	Redis struct {
		URL        string   `mapstructure:"url"`
		Servers    []string `mapstructure:"servers"`
		Cluster    bool     `mapstructure:"cluster"`
		MasterName string   `mapstructure:"master_name"`
		Password   string   `mapstructure:"password"`
		Database   int      `mapstructure:"database"`
		TLSEnabled bool     `mapstructure:"tls_enabled"`
		KeyPrefix  string   `mapstructure:"key_prefix"`
	} `mapstructure:"redis"`
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&csConfigFile, "cs-config-file", "", "", "Path to chirpstack.toml configuration file")
	rootCmd.PersistentFlags().StringVarP(&asConfigFile, "as-config-file", "", "", "Path to chirpstack-application-server.toml configuration file")
	rootCmd.PersistentFlags().StringArrayVarP(&nsConfigFiles, "ns-config-file", "", []string{}, "Path to chirpstack-network-server.toml configuration file (can be repeated)")
	rootCmd.PersistentFlags().IntVarP(&csSessionTTL, "device-session-ttl-days", "", 31, "Device-session TTL in days")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}

func run(cmd *cobra.Command, args []string) error {
	viper.SetConfigType("toml")

	log.Printf("Reading ChirpStack configuration file: %s", csConfigFile)
	csConfig := getConfig(csConfigFile, true)
	csRedis = getRedisClient(csConfig)
	csDB = getPostgresClient(csConfig)
	csPrefix = csConfig.Redis.KeyPrefix

	log.Printf("Reading AS configuration file: %s", asConfigFile)
	asConfig := getConfig(asConfigFile, false)
	asRedis = getRedisClient(asConfig)
	asDB = getPostgresClient(asConfig)
	asPrefix = asConfig.Redis.KeyPrefix

	deleteUsersAndTenants()
	migrateUsers()
	migrateOrganizations()
	migrateOrganizationUsers()
	migrateApplications()

	for _, nsConfigFile := range nsConfigFiles {
		log.Printf("Reading NS configuration file: %s", nsConfigFile)
		nsConfig := getConfig(nsConfigFile, false)
		nsRedis = getRedisClient(nsConfig)
		nsDB = getPostgresClient(nsConfig)
		nsPrefix = nsConfig.Redis.KeyPrefix

		migrateGateways()
		migrateDeviceProfiles()
		migrateDevices()
	}

	return nil
}

func getConfig(f string, parseRedis bool) Config {
	b, err := ioutil.ReadFile(f)
	if err != nil {
		panic(err)
	}
	if err := viper.ReadConfig(bytes.NewBuffer(b)); err != nil {
		panic(err)
	}
	config := Config{}
	if err := viper.Unmarshal(&config); err != nil {
		panic(err)
	}

	if config.Redis.URL != "" {
		opt, err := redis.ParseURL(config.Redis.URL)
		if err != nil {
			panic(err)
		}
		config.Redis.Servers = append(config.Redis.Servers, opt.Addr)
		config.Redis.Database = opt.DB
		config.Redis.Password = opt.Password
	}

	if parseRedis {
		for i, redisURL := range config.Redis.Servers {
			opt, err := redis.ParseURL(redisURL)
			if err != nil {
				panic(err)
			}

			config.Redis.Servers[i] = opt.Addr
			config.Redis.Database = opt.DB
			config.Redis.Password = opt.Password
			if opt.TLSConfig != nil {
				config.Redis.TLSEnabled = true
			}
		}
	}

	return config
}

func getRedisClient(c Config) redis.UniversalClient {
	var redisClient redis.UniversalClient
	var tlsConfig *tls.Config
	if c.Redis.TLSEnabled {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	if c.Redis.Cluster {
		redisClient = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:     c.Redis.Servers,
			Password:  c.Redis.Password,
			TLSConfig: tlsConfig,
		})
	} else if c.Redis.MasterName != "" {
		redisClient = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       c.Redis.MasterName,
			SentinelAddrs:    c.Redis.Servers,
			SentinelPassword: c.Redis.Password,
			DB:               c.Redis.Database,
			Password:         c.Redis.Password,
			TLSConfig:        tlsConfig,
		})
	} else {
		redisClient = redis.NewClient(&redis.Options{
			Addr:      c.Redis.Servers[0],
			DB:        c.Redis.Database,
			Password:  c.Redis.Password,
			TLSConfig: tlsConfig,
		})
	}

	return redisClient
}

func getPostgresClient(c Config) *sqlx.DB {
	d, err := sqlx.Open("postgres", c.PostgreSQL.DSN)
	if err != nil {
		panic(err)
	}
	return d
}

func deleteUsersAndTenants() {
	log.Println("Deleting users and tenants from target database")
	_, err := csDB.Exec("delete from tenant")
	if err != nil {
		panic(err)
	}
	_, err = csDB.Exec(`delete from "user"`)
	if err != nil {
		panic(err)
	}
}

func migrateUsers() {
	log.Println("Migrating users")
	type User struct {
		ID            int64     `db:"id"`
		IsAdmin       bool      `db:"is_admin"`
		IsActive      bool      `db:"is_active"`
		SessionTTL    int32     `db:"session_ttl"`
		CreatedAt     time.Time `db:"created_at"`
		UpdatedAt     time.Time `db:"updated_at"`
		PasswordHash  string    `db:"password_hash"`
		Email         string    `db:"email"`
		EmailVerified bool      `db:"email_verified"`
		EmailOld      string    `db:"email_old"`
		Note          string    `db:"note"`
		ExternalID    *string   `db:"external_id"`
	}

	users := []User{}
	if err := asDB.Select(&users, `select * from "user"`); err != nil {
		panic(err)
	}

	for _, user := range users {
		_, err := csDB.Exec(`
			insert into "user" (
				id,
				external_id,
				created_at,
				updated_at,
				is_admin,
				is_active,
				email,
				email_verified,
				password_hash,
				note
			) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			on conflict do nothing`,
			intToUUID(user.ID),
			user.ExternalID,
			user.CreatedAt,
			user.UpdatedAt,
			user.IsAdmin,
			user.IsActive,
			user.Email,
			user.EmailVerified,
			migratePassword(user.PasswordHash),
			user.Note,
		)
		if err != nil {
			panic(err)
		}
	}
}

func migrateOrganizations() {
	log.Println("Migrating organizations")
	type Organization struct {
		ID              int64     `db:"id"`
		CreatedAt       time.Time `db:"created_at"`
		UpdatedAt       time.Time `db:"updated_at"`
		Name            string    `db:"name"`
		DisplayName     string    `db:"display_name"`
		CanHaveGateways bool      `db:"can_have_gateways"`
		MaxDeviceCount  int       `db:"max_device_count"`
		MaxGatewayCount int       `db:"max_gateway_count"`
	}

	orgs := []Organization{}
	if err := asDB.Select(&orgs, "select * from organization"); err != nil {
		panic(err)
	}

	for _, org := range orgs {
		_, err := csDB.Exec(`
			insert into tenant (
				id,
				created_at,
				updated_at,
				name,
				description,
				can_have_gateways,
				max_device_count,
				max_gateway_count,
				private_gateways
			) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			on conflict do nothing`,
			intToUUID(org.ID),
			org.CreatedAt,
			org.UpdatedAt,
			org.Name,
			org.DisplayName,
			org.CanHaveGateways,
			org.MaxDeviceCount,
			org.MaxGatewayCount,
			false,
		)
		if err != nil {
			panic(err)
		}
	}
}

func migrateOrganizationUsers() {
	log.Println("Migrating organization users")
	type OrganizationUser struct {
		ID             int64     `db:"id"`
		UserID         int64     `db:"user_id"`
		OrganizationID int64     `db:"organization_id"`
		Email          string    `db:"email"`
		IsAdmin        bool      `db:"is_admin"`
		IsDeviceAdmin  bool      `db:"is_device_admin"`
		IsGatewayAdmin bool      `db:"is_gateway_admin"`
		CreatedAt      time.Time `db:"created_at"`
		UpdatedAt      time.Time `db:"updated_at"`
	}

	orgUsers := []OrganizationUser{}
	if err := asDB.Select(&orgUsers, "select * from organization_user"); err != nil {
		panic(err)
	}

	for _, orgUser := range orgUsers {
		_, err := csDB.Exec(`
			insert into tenant_user (
				tenant_id,
				user_id,
				created_at,
				updated_at,
				is_admin,
				is_device_admin,
				is_gateway_admin
			) values ($1, $2, $3, $4, $5, $6, $7)
			on conflict do nothing`,
			intToUUID(orgUser.OrganizationID),
			intToUUID(orgUser.UserID),
			orgUser.CreatedAt,
			orgUser.UpdatedAt,
			orgUser.IsAdmin,
			orgUser.IsDeviceAdmin,
			orgUser.IsGatewayAdmin,
		)
		if err != nil {
			panic(err)
		}
	}

}

func migrateApplications() {
	log.Println("Migrating applications")
	type Application struct {
		ID             int64  `db:"id"`
		Name           string `db:"name"`
		Description    string `db:"description"`
		OrganizationID int64  `db:"organization_id"`
		MQTTTLSCert    []byte `db:"mqtt_tls_cert"`
	}

	apps := []Application{}
	if err := asDB.Select(&apps, "select id, name, description, organization_id, mqtt_tls_cert from application"); err != nil {
		panic(err)
	}
	for _, app := range apps {
		_, err := csDB.Exec(`
			insert into application (
				id,
				tenant_id,
				created_at,
				updated_at,
				name,
				description,
				mqtt_tls_cert
			) values ($1, $2, $3, $4, $5, $6, $7)
			on conflict do nothing`,
			intToUUID(app.ID),
			intToUUID(app.OrganizationID),
			time.Now(),
			time.Now(),
			app.Name,
			app.Description,
			app.MQTTTLSCert,
		)
		if err != nil {
			panic(err)
		}
	}
}

func migrateGateways() {
	log.Println("Migrating gateways")

	type NSGateway struct {
		GatewayID        []byte     `db:"gateway_id"`
		RoutingProfileID uuid.UUID  `db:"routing_profile_id"`
		ServiceProfileID *uuid.UUID `db:"service_profile_id"`
		GatewayProfileID *uuid.UUID `db:"gateway_profile_id"`
		CreatedAt        time.Time  `db:"created_at"`
		UpdatedAt        time.Time  `db:"updated_at"`
		FirstSeenAt      *time.Time `db:"first_seen_at"`
		LastSeenAt       *time.Time `db:"last_seen_at"`
		Location         GPSPoint   `db:"location"`
		Altitude         float64    `db:"altitude"`
		TLSCert          []byte     `db:"tls_cert"`
	}

	type ASGateway struct {
		MAC              []byte        `db:"mac"`
		CreatedAt        time.Time     `db:"created_at"`
		UpdatedAt        time.Time     `db:"updated_at"`
		FirstSeenAt      *time.Time    `db:"first_seen_at"`
		LastSeenAt       *time.Time    `db:"last_seen_at"`
		Name             string        `db:"name"`
		Description      string        `db:"description"`
		OrganizationID   int64         `db:"organization_id"`
		Ping             bool          `db:"ping"`
		LastPingID       *int64        `db:"last_ping_id"`
		LastPingSentAt   *time.Time    `db:"last_ping_sent_at"`
		NetworkServerID  int64         `db:"network_server_id"`
		GatewayProfileID *uuid.UUID    `db:"gateway_profile_id"`
		ServiceProfileID *uuid.UUID    `db:"service_profile_id"`
		Latitude         float64       `db:"latitude"`
		Longitude        float64       `db:"longitude"`
		Altitude         float64       `db:"altitude"`
		Tags             hstore.Hstore `db:"tags"`
		Metadata         hstore.Hstore `db:"metadata"`
	}

	nsGateways := []NSGateway{}
	err := nsDB.Select(&nsGateways, "select * from gateway")
	if err != nil {
		panic(err)
	}

	for _, gw := range nsGateways {
		asGateway := ASGateway{}
		if err := asDB.Get(&asGateway, "select * from gateway where mac = $1", gw.GatewayID); err != nil {
			panic(err)
		}

		_, err = csDB.Exec(`
			insert into gateway (
				gateway_id,
				tenant_id,
				created_at,
				updated_at,
				last_seen_at,
				name,
				description,
				latitude,
				longitude,
				altitude,
				stats_interval_secs,
				tls_certificate,
				tags,
				properties
			) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
			gw.GatewayID,
			intToUUID(asGateway.OrganizationID),
			asGateway.CreatedAt,
			asGateway.UpdatedAt,
			asGateway.LastSeenAt,
			asGateway.Name,
			asGateway.Description,
			asGateway.Latitude,
			asGateway.Longitude,
			asGateway.Altitude,
			30,
			gw.TLSCert,
			hstoreToJSON(asGateway.Tags),
			hstoreToJSON(asGateway.Metadata),
		)
		if err != nil {
			panic(err)
		}

		migrateGatewayMetrics(gw.GatewayID)
	}
}

func migrateDeviceProfiles() {
	log.Println("Migrating device-profiles")

	type NSDeviceProfile struct {
		CreatedAt          time.Time   `db:"created_at"`
		UpdatedAt          time.Time   `db:"updated_at"`
		ID                 uuid.UUID   `db:"device_profile_id"`
		SupportsClassB     bool        `db:"supports_class_b"`
		ClassBTimeout      int         `db:"class_b_timeout"` // Unit: seconds
		PingSlotPeriod     int         `db:"ping_slot_period"`
		PingSlotDR         int         `db:"ping_slot_dr"`
		PingSlotFreq       uint32      `db:"ping_slot_freq"` // in Hz
		SupportsClassC     bool        `db:"supports_class_c"`
		ClassCTimeout      int         `db:"class_c_timeout"`     // Unit: seconds
		MACVersion         string      `db:"mac_version"`         // Example: "1.0.2" [LW102]
		RegParamsRevision  string      `db:"reg_params_revision"` // Example: "B" [RP102B]
		RXDelay1           int         `db:"rx_delay_1"`
		RXDROffset1        int         `db:"rx_dr_offset_1"`
		RXDataRate2        int         `db:"rx_data_rate_2"`       // Unit: bits-per-second
		RXFreq2            uint32      `db:"rx_freq_2"`            // In Hz
		FactoryPresetFreqs interface{} `db:"factory_preset_freqs"` // In Hz
		MaxEIRP            int         `db:"max_eirp"`             // In dBm
		MaxDutyCycle       int         `db:"max_duty_cycle"`       // Example: 10 indicates 10%
		SupportsJoin       bool        `db:"supports_join"`
		RFRegion           string      `db:"rf_region"`
		Supports32bitFCnt  bool        `db:"supports_32bit_fcnt"`
		ADRAlgorithmID     string      `db:"adr_algorithm_id"`
	}

	type ASDeviceProfile struct {
		DeviceProfileID      uuid.UUID     `db:"device_profile_id"`
		NetworkServerID      int64         `db:"network_server_id"`
		OrganizationID       int64         `db:"organization_id"`
		CreatedAt            time.Time     `db:"created_at"`
		UpdatedAt            time.Time     `db:"updated_at"`
		Name                 string        `db:"name"`
		PayloadCodec         string        `db:"payload_codec"`
		PayloadEncoderScript string        `db:"payload_encoder_script"`
		PayloadDecoderScript string        `db:"payload_decoder_script"`
		Tags                 hstore.Hstore `db:"tags"`
		UplinkInterval       time.Duration `db:"uplink_interval"`
	}

	nsDevProfiles := []NSDeviceProfile{}
	err := nsDB.Select(&nsDevProfiles, "select * from device_profile")
	if err != nil {
		panic(err)
	}

	for _, nsDP := range nsDevProfiles {
		asDP := ASDeviceProfile{}
		if err := asDB.Get(&asDP, "select * from device_profile where device_profile_id = $1", nsDP.ID); err != nil {
			panic(err)
		}

		_, err = csDB.Exec(`
			insert into device_profile (
				id,
				tenant_id,
				created_at,
				updated_at,
				name,
				region,
				mac_version,
				reg_params_revision,
				adr_algorithm_id,
				payload_codec_runtime,
				payload_encoder_config,
				payload_decoder_config,
				uplink_interval,
				supports_otaa,
				supports_class_b,
				supports_class_c,
				class_b_timeout,
				class_b_ping_slot_period,
				class_b_ping_slot_dr,
				class_b_ping_slot_freq,
				class_c_timeout,
				abp_rx1_delay,
				abp_rx1_dr_offset,
				abp_rx2_dr,
				abp_rx2_freq,
				tags,
				device_status_req_interval
			) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27)`,
			nsDP.ID,
			intToUUID(asDP.OrganizationID),
			asDP.CreatedAt,
			asDP.UpdatedAt,
			asDP.Name,
			nsDP.RFRegion,
			nsDP.MACVersion,
			nsDP.RegParamsRevision,
			nsDP.ADRAlgorithmID,
			asDP.PayloadCodec,
			asDP.PayloadEncoderScript,
			asDP.PayloadDecoderScript,
			asDP.UplinkInterval/time.Second,
			nsDP.SupportsJoin,
			nsDP.SupportsClassB,
			nsDP.SupportsClassC,
			nsDP.ClassBTimeout,
			nsDP.PingSlotPeriod,
			nsDP.PingSlotDR,
			nsDP.PingSlotFreq,
			nsDP.ClassCTimeout,
			nsDP.RXDelay1,
			nsDP.RXDROffset1,
			nsDP.RXDataRate2,
			nsDP.RXFreq2,
			hstoreToJSON(asDP.Tags),
			0,
		)
		if err != nil {
			panic(err)
		}
	}

}

func migrateDevices() {
	log.Println("Migrating devices")

	type NSDevice struct {
		DevEUI            []byte    `db:"dev_eui"`
		CreatedAt         time.Time `db:"created_at"`
		UpdatedAt         time.Time `db:"updated_at"`
		DeviceProfileID   uuid.UUID `db:"device_profile_id"`
		ServiceProfileID  uuid.UUID `db:"service_profile_id"`
		RoutingProfileID  uuid.UUID `db:"routing_profile_id"`
		SkipFCntCheck     bool      `db:"skip_fcnt_check"`
		ReferenceAltitude float64   `db:"reference_altitude"`
		Mode              string    `db:"mode"`
		IsDisabled        bool      `db:"is_disabled"`
	}

	type ASDevice struct {
		DevEUI                    []byte        `db:"dev_eui"`
		CreatedAt                 time.Time     `db:"created_at"`
		UpdatedAt                 time.Time     `db:"updated_at"`
		LastSeenAt                *time.Time    `db:"last_seen_at"`
		ApplicationID             int64         `db:"application_id"`
		DeviceProfileID           uuid.UUID     `db:"device_profile_id"`
		Name                      string        `db:"name"`
		Description               string        `db:"description"`
		SkipFCntCheck             bool          `db:"-"`
		ReferenceAltitude         float64       `db:"-"`
		DeviceStatusBattery       *float32      `db:"device_status_battery"`
		DeviceStatusMargin        *int          `db:"device_status_margin"`
		DeviceStatusExternalPower bool          `db:"device_status_external_power_source"`
		DR                        *int          `db:"dr"`
		Latitude                  *float64      `db:"latitude"`
		Longitude                 *float64      `db:"longitude"`
		Altitude                  *float64      `db:"altitude"`
		DevAddr                   []byte        `db:"dev_addr"`
		AppSKey                   []byte        `db:"app_s_key"`
		Variables                 hstore.Hstore `db:"variables"`
		Tags                      hstore.Hstore `db:"tags"`
		IsDisabled                bool          `db:"-"`
	}

	devices := []NSDevice{}
	err := nsDB.Select(&devices, "select * from device")
	if err != nil {
		panic(err)
	}

	for _, dev := range devices {
		// Device
		asDEV := ASDevice{}
		err := asDB.Get(&asDEV, "select * from device where dev_eui = $1", dev.DevEUI)
		if err != nil {
			panic(err)
		}

		_, err = csDB.Exec(`
			insert into device (
				dev_eui,
				application_id,
				device_profile_id,
				created_at,
				updated_at,
				last_seen_at,
				scheduler_run_after,
				name,
				description,
				external_power_source,
				battery_level,
				margin,
				dr,
				latitude,
				longitude,
				altitude,
				dev_addr,
				enabled_class,
				skip_fcnt_check,
				is_disabled,
				tags,
				variables
			) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)`,
			dev.DevEUI,
			intToUUID(asDEV.ApplicationID),
			asDEV.DeviceProfileID,
			asDEV.CreatedAt,
			asDEV.UpdatedAt,
			asDEV.LastSeenAt,
			time.Now(),
			asDEV.Name,
			asDEV.Description,
			asDEV.DeviceStatusExternalPower,
			asDEV.DeviceStatusBattery,
			asDEV.DeviceStatusMargin,
			asDEV.DR,
			asDEV.Latitude,
			asDEV.Longitude,
			asDEV.Altitude,
			asDEV.DevAddr,
			dev.Mode,
			dev.SkipFCntCheck,
			dev.IsDisabled,
			hstoreToJSON(asDEV.Tags),
			hstoreToJSON(asDEV.Variables),
		)
		if err != nil {
			panic(err)
		}

		// Migrate device queue
		migrateDeviceQueue(asDEV.DevEUI, asDEV.AppSKey)

		// Migrate metrics
		migrateDeviceMetrics(dev.DevEUI)

		// Device session
		ds, err := getDeviceSession(dev.DevEUI)
		if err != nil {
			// Device-session not found error
			continue
		}
		saveDeviceSession(ds)

		// Device - gateway
		devGW, err := getDeviceGateway(dev.DevEUI)
		if err != nil {
			continue
		}
		saveDeviceGateway(devGW)
	}
}

func migrateDeviceQueue(devEUI []byte, appSKeyB []byte) {
	log.Println("Migrating device-queue")
	var appSKey lorawan.AES128Key
	copy(appSKey[:], appSKeyB)

	type DeviceQueueItem struct {
		ID                      int64          `db:"id"`
		CreatedAt               time.Time      `db:"created_at"`
		UpdatedAt               time.Time      `db:"updated_at"`
		DevAddr                 []byte         `db:"dev_addr"`
		DevEUI                  []byte         `db:"dev_eui"`
		FRMPayload              []byte         `db:"frm_payload"`
		FCnt                    uint32         `db:"f_cnt"`
		FPort                   uint8          `db:"f_port"`
		Confirmed               bool           `db:"confirmed"`
		IsPending               bool           `db:"is_pending"`
		EmitAtTimeSinceGPSEpoch *time.Duration `db:"emit_at_time_since_gps_epoch"`
		TimeoutAfter            *time.Time     `db:"timeout_after"`
		RetryAfter              *time.Time     `db:"retry_after"`
	}

	queueItems := []DeviceQueueItem{}
	err := nsDB.Select(&queueItems, "select * from device_queue where dev_eui = $1", devEUI)
	if err != nil {
		panic(err)
	}

	for _, qi := range queueItems {
		var devAddr lorawan.DevAddr
		copy(devAddr[:], qi.DevAddr)

		pt, err := lorawan.EncryptFRMPayload(appSKey, false, devAddr, qi.FCnt, qi.FRMPayload)
		if err != nil {
			panic(err)
		}

		_, err = csDB.Exec(`
			insert into device_queue_item (
				id,
				dev_eui,
				created_at,
				f_port,
				confirmed,
				data,
				is_pending,
				f_cnt_down,
				timeout_after
			) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			intToUUID(qi.ID),
			qi.DevEUI,
			qi.CreatedAt,
			qi.FPort,
			qi.Confirmed,
			pt,
			qi.IsPending,
			qi.FCnt,
			qi.TimeoutAfter,
		)
		if err != nil {
			panic(err)
		}
	}
}

func intToUUID(id int64) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", id)
}

func hstoreToJSON(h hstore.Hstore) string {
	out := make(map[string]string)
	for k, v := range h.Map {
		out[k] = v.String
	}

	b, err := json.Marshal(out)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func getDeviceSession(devEUI []byte) (pbnew.DeviceSession, error) {
	key := fmt.Sprintf("%slora:ns:device:%s", nsPrefix, hex.EncodeToString(devEUI))
	var dsOld pbold.DeviceSessionPB

	val, err := nsRedis.Get(context.Background(), key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return pbnew.DeviceSession{}, err
		} else {
			panic(err)
		}

	}

	err = proto.Unmarshal(val, &dsOld)
	if err != nil {
		panic(err)
	}

	dsNew := pbnew.DeviceSession{
		DevEui:      dsOld.DevEui,
		DevAddr:     dsOld.DevAddr,
		JoinEui:     dsOld.JoinEui,
		FNwkSIntKey: dsOld.FNwkSIntKey,
		SNwkSIntKey: dsOld.SNwkSIntKey,
		NwkSEncKey:  dsOld.NwkSEncKey,
		AppSKey: &pbnew.KeyEnvelope{
			KekLabel: dsOld.GetAppSKeyEnvelope().GetKekLabel(),
			AesKey:   dsOld.GetAppSKeyEnvelope().GetAesKey(),
		},
		FCntUp:                      dsOld.FCntUp,
		NFCntDown:                   dsOld.NFCntDown,
		AFCntDown:                   dsOld.AFCntDown,
		ConfFCnt:                    dsOld.ConfFCnt,
		SkipFCntCheck:               dsOld.SkipFCntCheck,
		Rx1Delay:                    dsOld.RxDelay,
		Rx1DrOffset:                 dsOld.Rx1DrOffset,
		Rx2Dr:                       dsOld.Rx2Dr,
		Rx2Frequency:                dsOld.Rx2Frequency,
		EnabledUplinkChannelIndices: dsOld.EnabledUplinkChannels,
		ExtraUplinkChannels:         make(map[uint32]*pbnew.DeviceSessionChannel),
		ClassBPingSlotDr:            dsOld.PingSlotDr,
		ClassBPingSlotFreq:          dsOld.PingSlotFrequency,
		ClassBPingSlotNb:            dsOld.PingSlotNb,
		NbTrans:                     dsOld.NbTrans,
		TxPowerIndex:                dsOld.TxPowerIndex,
		Dr:                          dsOld.Dr,
		Adr:                         dsOld.Adr,
		MaxSupportedTxPowerIndex:    dsOld.MaxSupportedTxPowerIndex,
		MinSupportedTxPowerIndex:    dsOld.MinSupportedTxPowerIndex,
		UplinkAdrHistory:            []*pbnew.UplinkAdrHistory{},
		MacCommandErrorCount:        dsOld.MacCommandErrorCount,
		RejoinRequestEnabled:        dsOld.RejoinRequestEnabled,
		RejoinRequestMaxTimeN:       dsOld.RejoinRequestMaxTimeN,
		RejoinRequestMaxCountN:      dsOld.RejoinRequestMaxCountN,
		RejoinCount_0:               dsOld.RejoinCount_0,
		UplinkDwellTime_400Ms:       dsOld.UplinkDwellTime_400Ms,
		DownlinkDwellTime_400Ms:     dsOld.DownlinkDwellTime_400Ms,
		UplinkMaxEirpIndex:          dsOld.UplinkMaxEirpIndex,
	}

	switch dsOld.MacVersion {
	case "1.0.0":
		dsNew.MacVersion = pbnew.MacVersion_LORAWAN_1_0_0
	case "1.0.1":
		dsNew.MacVersion = pbnew.MacVersion_LORAWAN_1_0_1
	case "1.0.2":
		dsNew.MacVersion = pbnew.MacVersion_LORAWAN_1_0_2
	case "1.0.3":
		dsNew.MacVersion = pbnew.MacVersion_LORAWAN_1_0_3
	case "1.0.4":
		dsNew.MacVersion = pbnew.MacVersion_LORAWAN_1_0_4
	case "1.1.0":
		dsNew.MacVersion = pbnew.MacVersion_LORAWAN_1_1_0
	case "1.1.1":
		dsNew.MacVersion = pbnew.MacVersion_LORAWAN_1_1_0
	}

	for k, v := range dsOld.ExtraUplinkChannels {
		dsNew.ExtraUplinkChannels[k] = &pbnew.DeviceSessionChannel{
			Frequency: v.Frequency,
			MinDr:     v.MinDr,
			MaxDr:     v.MaxDr,
		}
	}

	for _, v := range dsOld.UplinkAdrHistory {
		dsNew.UplinkAdrHistory = append(dsNew.UplinkAdrHistory, &pbnew.UplinkAdrHistory{
			FCnt:         v.FCnt,
			MaxSnr:       v.MaxSnr,
			TxPowerIndex: v.TxPowerIndex,
			GatewayCount: v.GatewayCount,
		})
	}

	return dsNew, nil
}

func saveDeviceSession(ds pbnew.DeviceSession) {
	devAddrKey := fmt.Sprintf("%sdevaddr:%s", csPrefix, hex.EncodeToString(ds.DevAddr))
	devSessKey := fmt.Sprintf("%sdevice:%s:ds", csPrefix, hex.EncodeToString(ds.DevEui))

	b, err := proto.Marshal(&ds)
	if err != nil {
		panic(err)
	}

	pipe := csRedis.TxPipeline()
	pipe.SAdd(context.Background(), devAddrKey, hex.EncodeToString(ds.DevEui))
	pipe.PExpire(context.Background(), devAddrKey, time.Duration(csSessionTTL)*time.Hour*24)
	if _, err := pipe.Exec(context.Background()); err != nil {
		panic(err)
	}

	err = csRedis.Set(context.Background(), devSessKey, b, time.Duration(csSessionTTL)*time.Hour*24).Err()
	if err != nil {
		panic(err)
	}
}

func getDeviceGateway(devEUI []byte) (pbnew.DeviceGatewayRxInfo, error) {
	key := fmt.Sprintf("%slora:ns:device:%s:gwrx", nsPrefix, hex.EncodeToString(devEUI))
	var devGWOld pbold.DeviceGatewayRXInfoSetPB

	val, err := nsRedis.Get(context.Background(), key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return pbnew.DeviceGatewayRxInfo{}, err
		} else {
			panic(err)
		}

	}

	err = proto.Unmarshal(val, &devGWOld)
	if err != nil {
		panic(err)
	}

	devGW := pbnew.DeviceGatewayRxInfo{
		DevEui: devGWOld.DevEui,
		Dr:     devGWOld.Dr,
		Items:  []*pbnew.DeviceGatewayRxInfoItem{},
	}

	for i := range devGWOld.Items {
		devGW.Items = append(devGW.Items, &pbnew.DeviceGatewayRxInfoItem{
			GatewayId: devGWOld.Items[i].GatewayId,
			Rssi:      devGWOld.Items[i].Rssi,
			LoraSnr:   float32(devGWOld.Items[i].LoraSnr),
			Board:     devGWOld.Items[i].Board,
			Antenna:   devGWOld.Items[i].Antenna,
			Context:   devGWOld.Items[i].Context,
		})
	}

	return devGW, nil
}

func saveDeviceGateway(devGW pbnew.DeviceGatewayRxInfo) {
	key := fmt.Sprintf("%sdevice:%s:gwrx", nsPrefix, hex.EncodeToString(devGW.DevEui))
	b, err := proto.Marshal(&devGW)
	if err != nil {
		panic(err)
	}

	err = csRedis.Set(context.Background(), key, b, time.Duration(csSessionTTL)*time.Hour*24).Err()
	if err != nil {
		panic(err)
	}
}

func migrateDeviceMetrics(devEUI []byte) {
	key := fmt.Sprintf("%slora:as:metrics:{device:%s}:*", asPrefix, hex.EncodeToString(devEUI))
	keys, err := asRedis.Keys(context.Background(), key).Result()
	if err != nil {
		panic(err)
	}

	for _, key := range keys {
		vals, err := asRedis.HGetAll(context.Background(), key).Result()
		if err != nil {
			panic(err)
		}

		keyParts := strings.Split(key, ":")
		aggregation := keyParts[len(keyParts)-2]
		ts := keyParts[len(keyParts)-1]
		tsInt, err := strconv.Atoi(ts)
		if err != nil {
			panic(err)
		}
		tsDate := time.Unix(int64(tsInt), 0)
		tsStr := tsDate.Format("200601021504")
		newKey := fmt.Sprintf("%smetrics:{device:%s}:%s:%s", csPrefix, hex.EncodeToString(devEUI), aggregation, tsStr)

		ttl := map[string]time.Duration{
			"HOUR":  time.Hour * 24 * 2,
			"DAY":   time.Hour * 24 * 31 * 2,
			"MONTH": time.Hour * 24 * 31 * 365 * 2,
		}

		if err := csRedis.HSet(context.Background(), newKey, vals).Err(); err != nil {
			panic(err)
		}
		if err := csRedis.PExpire(context.Background(), newKey, ttl[aggregation]).Err(); err != nil {
			panic(err)
		}
	}
}

func migrateGatewayMetrics(gatewayID []byte) {
	key := fmt.Sprintf("%slora:as:metrics:{gw:%s}:*", asPrefix, hex.EncodeToString(gatewayID))
	keys, err := asRedis.Keys(context.Background(), key).Result()
	if err != nil {
		panic(err)
	}

	for _, key := range keys {
		vals, err := asRedis.HGetAll(context.Background(), key).Result()
		if err != nil {
			panic(err)
		}

		keyParts := strings.Split(key, ":")
		aggregation := keyParts[len(keyParts)-2]
		ts := keyParts[len(keyParts)-1]
		tsInt, err := strconv.Atoi(ts)
		if err != nil {
			panic(err)
		}
		tsDate := time.Unix(int64(tsInt), 0)
		tsStr := tsDate.Format("200601021504")
		newKey := fmt.Sprintf("%smetrics:{gw:%s}:%s:%s", csPrefix, hex.EncodeToString(gatewayID), aggregation, tsStr)

		ttl := map[string]time.Duration{
			"HOUR":  time.Hour * 24 * 2,
			"DAY":   time.Hour * 24 * 31 * 2,
			"MONTH": time.Hour * 24 * 31 * 365 * 2,
		}

		if err := csRedis.HSet(context.Background(), newKey, vals).Err(); err != nil {
			panic(err)
		}
		if err := csRedis.PExpire(context.Background(), newKey, ttl[aggregation]).Err(); err != nil {
			panic(err)
		}
	}
}

// GPSPoint contains a GPS point.
type GPSPoint struct {
	Latitude  float64
	Longitude float64
}

// Value implements the driver.Valuer interface.
func (l GPSPoint) Value() (driver.Value, error) {
	return fmt.Sprintf("(%s,%s)", strconv.FormatFloat(l.Latitude, 'f', -1, 64), strconv.FormatFloat(l.Longitude, 'f', -1, 64)), nil
}

// Scan implements the sql.Scanner interface.
func (l *GPSPoint) Scan(src interface{}) error {
	b, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}

	_, err := fmt.Sscanf(string(b), "(%f,%f)", &l.Latitude, &l.Longitude)
	return err
}

func migratePassword(s string) string {
	// old:
	// PBKDF2$sha512$1$l8zGKtxRESq3PA2kFhHRWA==$H3lGMxOt55wjwoc+myeOoABofJY9oDpldJa7fhqdjbh700V6FLPML75UmBOt9J5VFNjAL1AvqCozA1HJM0QVGA==

	// new:
	// $pbkdf2-sha512$i=1,l=64$l8zGKtxRESq3PA2kFhHRWA$H3lGMxOt55wjwoc+myeOoABofJY9oDpldJa7fhqdjbh700V6FLPML75UmBOt9J5VFNjAL1AvqCozA1HJM0QVGA
	if s == "" {
		return s
	}

	parts := strings.SplitN(s, "$", 5)
	if len(parts) != 5 {
		panic("Invalid password hash " + s)
	}

	return fmt.Sprintf("$pbkdf2-%s$i=$%s,l=16$%s$%s", parts[1], parts[2], parts[3], parts[4])
}
