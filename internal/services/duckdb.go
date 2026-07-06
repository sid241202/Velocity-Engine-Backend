package services

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"velocity-engine-control-plane-backend-go/internal/config"

	_ "github.com/marcboeker/go-duckdb"
)

var icebergS3PathRegex = regexp.MustCompile(`^s3://[a-zA-Z0-9._/-]+$`)

// istZone is the IST fixed timezone offset (UTC+05:30). The Iceberg
// event_timestamp column is genuinely stored in UTC (unlike the ClickHouse
// path, where Flink writes naive IST strings) — converting query results to
// IST here for display is the correct, deliberate operation.
var istZone = time.FixedZone("IST", 5*60*60+30*60)

func init() {
	if !icebergS3PathRegex.MatchString(config.IcebergS3Path) {
		slog.Error("Invalid ICEBERG_S3_PATH format", "path", config.IcebergS3Path)
	}
}

// IcebergSchemaMapping maps Kafka JSON field names (as used by the UI)
// to Iceberg Parquet column names. This must be kept in sync with the
// Python ICEBERG_SCHEMA_MAPPING in duckdb_worker.py.
var IcebergSchemaMapping = map[string]string{
	"_event_timestamp":                          "event_timestamp",
	"_event_id":                                 "event_id",
	"_category":                                 "category",
	"_event_type":                               "event_type",
	"_version":                                  "version",
	"_data.authCode":                            "authcode",
	"_data.subRequestId":                        "subrequestid",
	"_data.aua":                                 "aua",
	"_data.sa":                                  "sa",
	"_data.asa":                                 "asa",
	"_data.ver":                                 "ver",
	"_data.tid":                                 "tid",
	"_data.licenseId":                           "licenseid",
	"_data.reqDateTime":                         "reqdatetime",
	"_data.tknUsedFlag":                         "tknusedflag",
	"_data.tknType":                             "tkntype",
	"_data.pidTs":                               "pidts",
	"_data.pidVersion":                          "pidversion",
	"_data.piUsesFlag":                          "piusesflag",
	"_data.paUsesFlag":                          "pausesflag",
	"_data.pfaUsesFlag":                         "pfausesflag",
	"_data.bioUsesFlag":                         "biousesflag",
	"_data.btFMRUsesFlag":                       "btfmrusesflag",
	"_data.btFIRUsesFlag":                       "btfirusesflag",
	"_data.btIIRUsesFlag":                       "btiirusesflag",
	"_data.pinUsesFlag":                         "pinusesflag",
	"_data.otpUsesFlag":                         "otpusesflag",
	"_data.piUsedFlag":                          "piusedflag",
	"_data.paUsedFlag":                          "pausedflag",
	"_data.pfaUsedFlag":                         "pfausedflag",
	"_data.bioUsedFlag":                         "biousedflag",
	"_data.btFMRUsedFlag":                       "btfmrusedflag",
	"_data.btFIRUsedFlag":                       "btfirusedflag",
	"_data.btIIRUsedFlag":                       "btiirusedflag",
	"_data.pinUsedFlag":                         "pinusedflag",
	"_data.otpUsedFlag":                         "otpusedflag",
	"_data.otpIdentifier":                       "otpidentifier",
	"_data.lang":                                "lang",
	"_data.piNameUsedFlag":                      "pinameusedflag",
	"_data.piMs":                                "pims",
	"_data.piMv":                                "pimv",
	"_data.piMatchScore":                        "pimatchscore",
	"_data.piLNameUsedFlag":                     "pilnameusedflag",
	"_data.piLNameMs":                           "pilnamems",
	"_data.piLNameMv":                           "pilnamemv",
	"_data.piLNameMatchScore":                   "pilnamematchscore",
	"_data.piPhoneUsedFlag":                     "piphoneusedflag",
	"_data.piEmailUsedFlag":                     "piemailusedflag",
	"_data.piGenderUsedFlag":                    "pigenderusedflag",
	"_data.piGender":                            "pigender",
	"_data.piDOBUsedFlag":                       "pidobusedflag",
	"_data.piDOB":                               "pidob",
	"_data.pi_DOBTUsedFlag":                     "pi_dobtusedflag",
	"_data.piDOBT":                              "pidobt",
	"_data.piAgeUsed_flag":                      "piageused_flag",
	"_data.piAge":                               "piage",
	"_data.pfaAddressUsedFlag":                  "pfaaddressusedflag",
	"_data.pfaAddressMs":                        "pfaaddressms",
	"_data.pfaAddressMv":                        "pfaaddressmv",
	"_data.pfaMatchScore":                       "pfamatchscore",
	"_data.pfaLaddrUsedFlag":                    "pfaladdrusedflag",
	"_data.pfaLocalAddressMs":                   "pfalocaladdressms",
	"_data.pfaLocalAddressMv":                   "pfalocaladdressmv",
	"_data.pfaLocalAddressMatcScore":            "pfalocaladdressmatcscore",
	"_data.paMs":                                "pams",
	"_data.paCareOfUsedFlag":                    "pacareofusedflag",
	"_data.paHouseUsedFlag":                     "pahouseusedflag",
	"_data.paStreetUsedFlag":                    "pastreetusedflag",
	"_data.paLmUsedFlag":                        "palmusedflag",
	"_data.paLOCUsedFlag":                       "palocusedflag",
	"_data.paVTCUsedFlag":                       "pavtcusedflag",
	"_data.paVTC":                               "pavtc",
	"_data.paPOUsedFlag":                        "papousedflag",
	"_data.paPO":                                "papo",
	"_data.paSubDistrictUsedFlag":               "pasubdistrictusedflag",
	"_data.paSubDistrict":                       "pasubdistrict",
	"_data.paDistrictUsedFlag":                  "padistrictusedflag",
	"_data.paDistrict":                          "padistrict",
	"_data.paStateUsedFlag":                     "pastateusedflag",
	"_data.paState":                             "pastate",
	"_data.paPCUsedFlag":                        "papcusedflag",
	"_data.paPC":                                "papc",
	"_data.fmrCount":                            "fmrcount",
	"_data.firCount":                            "fircount",
	"_data.iirCount":                            "iircount",
	"_data.fingerMatchScore":                    "fingermatchscore",
	"_data.uidaiTFMR":                           "uidaitfmr",
	"_data.auaTFMR":                             "auatfmr",
	"_data.fingerMatchThreshold":                "fingermatchthreshold",
	"_data.fmrGalleryType":                      "fmrgallerytype",
	"_data.fmrGalleryVendor":                    "fmrgalleryvendor",
	"_data.fmrSDKVendor":                        "fmrsdkvendor",
	"_data.fmrSDKVersion":                       "fmrsdkversion",
	"_data.firGalleryType":                      "firgallerytype",
	"_data.firGalleryVendor":                    "firgalleryvendor",
	"_data.firSDKVendor":                        "firsdkvendor",
	"_data.firSDKVersion":                       "firsdkversion",
	"_data.iirGalleryType":                      "iirgallerytype",
	"_data.iirGalleryVendor":                    "iirgalleryvendor",
	"_data.iirSDKVendor":                        "iirsdkvendor",
	"_data.iirSDKVersion":                       "iirsdkversion",
	"_data.authResult":                          "authresult",
	"_data.errorCode":                           "errorcode",
	"_data.errorClassification":                 "errorclassification",
	"_data.responseDateTime":                    "responsedatetime",
	"_data.authDuration":                        "authduration",
	"_data.fdc":                                 "fdc",
	"_data.idc":                                 "idc",
	"_data.udc":                                 "udc",
	"_data.locationLat":                         "locationlat",
	"_data.locationLong":                        "locationlong",
	"_data.locationVTCCode":                     "locationvtccode",
	"_data.locationSubDistrictCode":             "locationsubdistrictcode",
	"_data.locationDistrictCode":                "locationdistrictcode",
	"_data.locationStateCode":                   "locationstatecode",
	"_data.locationPC":                          "locationpc",
	"_data.enrolmentReferenceId":                "enrolmentreferenceid",
	"_data.residentGender":                      "residentgender",
	"_data.residentBirth_Day":                   "residentbirth_day",
	"_data.residentBirthMonth":                  "residentbirthmonth",
	"_data.residentBirthYear":                   "residentbirthyear",
	"_data.residentDOB":                         "residentdob",
	"_data.residentDOBT":                        "residentdobt",
	"_data.residentAge":                         "residentage",
	"_data.residentPincode":                     "residentpincode",
	"_data.residentVTCCode":                     "residentvtccode",
	"_data.residentVTCName":                     "residentvtcname",
	"_data.residentPOName":                      "residentponame",
	"_data.residentSubDistrictCode":             "residentsubdistrictcode",
	"_data.residentSubDistrictName":             "residentsubdistrictname",
	"_data.residentDistrictCode":                "residentdistrictcode",
	"_data.residentDistrictName":                "residentdistrictname",
	"_data.residentStateCode":                   "residentstatecode",
	"_data.residentStateName":                   "residentstatename",
	"_data.uidGenerationDate":                   "uidgenerationdate",
	"_data.txn":                                 "txn",
	"_data.authType":                            "authtype",
	"_data.hashUID":                             "hashuid",
	"_data.locationAlt":                         "locationalt",
	"_data.lot":                                 "lot",
	"_data.bfdDoneFlag":                         "bfddoneflag",
	"_data.fingerMatchingType":                  "fingermatchingtype",
	"_data.fingerFusionPerfomed":                "fingerfusionperfomed",
	"_data.irisMatchScore":                      "irismatchscore",
	"_data.irisThreshold":                       "iristhreshold",
	"_data.irisMatchingType":                    "irismatchingtype",
	"_data.irisFusionPerfomed":                  "irisfusionperfomed",
	"_data.authXMLSize":                         "authxmlsize",
	"_data.pidSize":                             "pidsize",
	"_data.dataType":                            "datatype",
	"_data.skeyScheme":                          "skeyscheme",
	"_data.sskType":                             "ssktype",
	"_data.ki":                                  "ki",
	"_data.kycFlag":                             "kycflag",
	"_data.actionCodes":                         "actioncodes",
	"_data.registeredDeviceSoftwareId":          "registereddevicesoftwareid",
	"_data.registeredDeviceSoftwareVersion":     "registereddevicesoftwareversion",
	"_data.deviceProviderId":                    "deviceproviderid",
	"_data.deviceCode":                          "devicecode",
	"_data.modelId":                             "modelid",
	"_data.certExpiryDate":                      "certexpirydate",
	"_data.subErrorCode":                        "suberrorcode",
	"_data.authenticationMode":                  "authenticationmode",
	"_data.faceUses":                            "faceuses",
	"_data.faceUsed":                            "faceused",
	"_data.faceMatchScore":                      "facematchscore",
	"_data.faceMatchThreshold":                  "facematchthreshold",
	"_data.faceMatchType":                       "facematchtype",
	"_data.faceFusionDone":                      "facefusiondone",
	"_data.faceMultimodalityFusionScore":        "facemultimodalityfusionscore",
	"_data.faceMultimodalityFusionThreshold":    "facemultimodalityfusionthreshold",
	"_data.faceSdkVendor":                       "facesdkvendor",
	"_data.faceSdkVersion":                      "facesdkversion",
	"_data.deviceMetaData":                      "devicemetadata",
	"_data.deepPrintGalleryType":                "deepprintgallerytype",
	"_data.deepPrintGalleryVendor":              "deepprintgalleryvendor",
	"_data.deepPrintSdkVendor":                  "deepprintsdkvendor",
	"_data.deepPrintSdkVersion":                 "deepprintsdkversion",
	"_data.deepPrintMatchScoreFinger1":          "deepprintmatchscorefinger1",
	"_data.deepPrintMatchScoreFinger2":          "deepprintmatchscorefinger2",
	"_data.deepPrintMatchIsPerfomed":            "deepprintmatchisperfomed",
	"_data.deepPrintMatchResult":                "deepprintmatchresult",
	"_data.serverId":                            "serverid",
}

// allowedColumns is the set of valid Iceberg column names for SQL injection prevention.
var allowedColumns map[string]bool

func init() {
	allowedColumns = make(map[string]bool)
	for _, v := range IcebergSchemaMapping {
		allowedColumns[v] = true
	}
	// Add extra allowed names
	for _, extra := range []string{"event_timestamp", "event_id", "category", "event_type", "version", "__GLOBAL__", "window_start"} {
		allowedColumns[extra] = true
	}
}

// TranslateField translates a Kafka JSON UI field name to an Iceberg column name.
func TranslateField(kafkaField string) string {
	if mapped, ok := IcebergSchemaMapping[kafkaField]; ok && mapped != "" {
		return mapped
	}
	return strings.Replace(kafkaField, "_data.", "", 1)
}

// ValidateIdentifier validates that a column name is safe for SQL use.
func ValidateIdentifier(name string) (string, error) {
	clean := strings.TrimSpace(strings.ToLower(name))
	if clean == "" {
		return "", fmt.Errorf("empty identifier")
	}
	// Check it's a valid identifier: starts with letter or underscore, contains only alphanumeric and underscores
	for i, r := range clean {
		if i == 0 {
			if !unicode.IsLetter(r) && r != '_' {
				return "", fmt.Errorf("invalid column identifier: %q", name)
			}
		} else {
			if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
				return "", fmt.Errorf("invalid column identifier: %q", name)
			}
		}
	}
	return clean, nil
}

// ParseFilterNode parses a filter node tree into a (sqlFragment, params) tuple.
// Uses DuckDB ? placeholders to prevent SQL injection.
// Returns ("", nil) if the node is empty or invalid.
func ParseFilterNode(node map[string]interface{}) (string, []interface{}) {
	if node == nil || len(node) == 0 {
		return "", nil
	}

	nodeType, _ := node["type"].(string)

	if nodeType == "group" {
		logic, _ := node["logic"].(string)
		if logic != "AND" && logic != "OR" {
			logic = "AND"
		}
		conditions, _ := node["conditions"].([]interface{})
		if len(conditions) == 0 {
			return "", nil
		}

		var parsedParts []struct {
			sql    string
			params []interface{}
		}

		for _, c := range conditions {
			cond, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			sqlFrag, params := ParseFilterNode(cond)
			if sqlFrag != "" {
				parsedParts = append(parsedParts, struct {
					sql    string
					params []interface{}
				}{sqlFrag, params})
			}
		}

		if len(parsedParts) == 0 {
			return "", nil
		}
		if len(parsedParts) == 1 {
			return parsedParts[0].sql, parsedParts[0].params
		}

		var sqlParts []string
		var allParams []interface{}
		for _, p := range parsedParts {
			sqlParts = append(sqlParts, p.sql)
			allParams = append(allParams, p.params...)
		}
		combined := "(" + strings.Join(sqlParts, " "+logic+" ") + ")"
		return combined, allParams

	} else if nodeType == "condition" {
		fieldRaw, _ := node["field"].(string)
		translated := TranslateField(fieldRaw)
		field, err := ValidateIdentifier(translated)
		if err != nil {
			slog.Warn("Invalid filter field", "field", fieldRaw, "error", err)
			return "", nil
		}

		op, _ := node["operator"].(string)
		val := node["value"]

		opMap := map[string]string{
			"EQUALS":             "=",
			"NOT_EQUALS":        "!=",
			"GREATER_THAN":      ">",
			"GREATER_THAN_EQUAL": ">=",
			"LESS_THAN":         "<",
			"LESS_THAN_EQUAL":   "<=",
		}

		if sqlOp, ok := opMap[op]; ok {
			return fmt.Sprintf("%s %s ?", field, sqlOp), []interface{}{val}
		} else if op == "IN" {
			var items []interface{}
			switch v := val.(type) {
			case []interface{}:
				items = v
			case string:
				for _, s := range strings.Split(v, ",") {
					items = append(items, strings.TrimSpace(s))
				}
			}
			if len(items) == 0 {
				return "", nil
			}
			placeholders := make([]string, len(items))
			for i := range items {
				placeholders[i] = "?"
			}
			return fmt.Sprintf("%s IN (%s)", field, strings.Join(placeholders, ", ")), items
		} else if op == "REGEX" {
			return fmt.Sprintf("regexp_matches(%s, ?)", field), []interface{}{val}
		} else if op == "CONTAINS" {
			return fmt.Sprintf("contains(CAST(%s AS VARCHAR), CAST(? AS VARCHAR))", field), []interface{}{val}
		} else if op == "NOT_CONTAINS" {
			return fmt.Sprintf("NOT contains(CAST(%s AS VARCHAR), CAST(? AS VARCHAR))", field), []interface{}{val}
		} else if op == "STARTS_WITH" {
			return fmt.Sprintf("starts_with(CAST(%s AS VARCHAR), CAST(? AS VARCHAR))", field), []interface{}{val}
		} else if op == "ENDS_WITH" {
			return fmt.Sprintf("ends_with(CAST(%s AS VARCHAR), CAST(? AS VARCHAR))", field), []interface{}{val}
		} else if op == "IS_NULL" {
			return fmt.Sprintf("%s IS NULL", field), nil
		} else if op == "IS_NOT_NULL" {
			return fmt.Sprintf("%s IS NOT NULL", field), nil
		} else if op == "DATE_BEFORE" || op == "DATE_AFTER" || op == "DATE_EQUALS" {
			formatRaw, _ := node["format"].(string)
			resolved, err := resolveDateFilterValue(val, formatRaw)
			if err != nil {
				slog.Warn("Invalid date filter value, dropping condition", "field", fieldRaw, "error", err)
				return "", nil
			}
			dateOpMap := map[string]string{"DATE_BEFORE": "<", "DATE_AFTER": ">", "DATE_EQUALS": "="}
			return fmt.Sprintf("try_cast(%s AS TIMESTAMP) %s try_cast(? AS TIMESTAMP)", field, dateOpMap[op]), []interface{}{resolved}
		}
	}

	return "", nil
}

// resolveDateFilterValue converts a DATE_BEFORE/DATE_AFTER/DATE_EQUALS filter's
// raw value into the naive-UTC "YYYY-MM-DD HH:MM:SS.sss" string this file binds
// against TIMESTAMP columns with. The frontend contract (VisualFilterBuilder.jsx)
// sends all date/time values as IST: EPOCH_MILLIS is an absolute instant (no
// conversion needed beyond parsing), while ISO_STRING carries an explicit
// "+05:30" offset that must be resolved before truncating to UTC — otherwise
// this reintroduces the same +5:30 class of bug the start_ts/end_ts handling
// above already had to work around.
func resolveDateFilterValue(rawVal interface{}, format string) (string, error) {
	valStr := strings.TrimSpace(fmt.Sprintf("%v", rawVal))
	switch format {
	case "EPOCH_MILLIS":
		ms, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return "", fmt.Errorf("invalid EPOCH_MILLIS value %q: %w", valStr, err)
		}
		return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05.000"), nil
	case "ISO_STRING", "":
		t, err := time.Parse(time.RFC3339, valStr)
		if err != nil {
			return "", fmt.Errorf("invalid ISO_STRING value %q: %w", valStr, err)
		}
		return t.UTC().Format("2006-01-02 15:04:05.000"), nil
	default:
		return "", fmt.Errorf("unknown date format %q", format)
	}
}

// safeHavingPattern is the regex for validating HAVING expressions.
var safeHavingPattern = regexp.MustCompile(
	`^[\s()a-zA-Z0-9_.><!=+\-*/]+(?:\s+(?:AND|OR|NOT)\s+[\s()a-zA-Z0-9_.><!=+\-*/]+)*$`,
)

// identTokenPattern extracts identifier tokens from an expression.
var identTokenPattern = regexp.MustCompile(`[a-zA-Z_][a-zA-Z0-9_]*`)

// sqlKeywords is the set of SQL keywords allowed in HAVING expressions.
var sqlKeywords = map[string]bool{
	"AND": true, "OR": true, "NOT": true,
	"true": true, "false": true, "TRUE": true, "FALSE": true,
	"CASE": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
}

// ParseHavingExpression validates and sanitizes a HAVING expression string.
func ParseHavingExpression(expr string, validAliases map[string]bool) (string, error) {
	if expr == "" {
		return "", nil
	}

	// Replace JS-style operators with SQL
	expr = strings.ReplaceAll(expr, "&&", " AND ")
	expr = strings.ReplaceAll(expr, "||", " OR ")
	expr = strings.ReplaceAll(expr, "==", "=")

	cleaned := strings.TrimSpace(expr)
	if cleaned == "" {
		return "", nil
	}

	if !safeHavingPattern.MatchString(cleaned) {
		return "", fmt.Errorf("unsafe HAVING expression rejected: %q", expr)
	}

	if validAliases != nil {
		tokens := identTokenPattern.FindAllString(cleaned, -1)
		for _, tok := range tokens {
			if sqlKeywords[tok] || sqlKeywords[strings.ToUpper(tok)] {
				continue
			}
			if !validAliases[tok] {
				return "", fmt.Errorf("unknown identifier in HAVING expression: %q", tok)
			}
		}
	}

	return cleaned, nil
}

// ─── DuckDB Singleton ────────────────────────────────────────────────────────
// A single persistent DuckDB connection is shared across all historical-analysis
// requests. This avoids the ~5–15 s overhead of re-installing and re-loading the
// iceberg + httpfs extensions on every HTTP call.
//
// All callers must hold duckDBMu for the entire S3-config + query sequence because
// DuckDB SET variables are connection-global and we enforce MaxOpenConns(1).

var (
	duckDBMu        sync.Mutex
	duckDBSingleton *sql.DB
)

// initDuckDB returns the shared DuckDB connection, creating it on the first call.
// It tries LOAD first (fast: uses the local extension cache) and falls back to
// INSTALL + LOAD on a cold start or missing cache (e.g., a fresh container).
func initDuckDB() (*sql.DB, error) {
	duckDBMu.Lock()
	defer duckDBMu.Unlock()

	if duckDBSingleton != nil {
		return duckDBSingleton, nil
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	// Apply settings individually so we can detect which one fails.
	if _, err = db.Exec("SET memory_limit='600MB'"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to set DuckDB memory_limit: %w", err)
	}
	if _, err = db.Exec("SET temp_directory='/tmp/duckdb'"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to set DuckDB temp_directory: %w", err)
	}
	// One connection only: DuckDB in-memory + global SET vars are not safe across
	// concurrent sessions.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	for _, ext := range []string{"iceberg", "httpfs"} {
		if _, loadErr := db.Exec("LOAD " + ext); loadErr != nil {
			slog.Info("DuckDB extension not in cache — installing", "ext", ext)
			if _, instErr := db.Exec("INSTALL " + ext); instErr != nil {
				_ = db.Close()
				return nil, fmt.Errorf("failed to install DuckDB extension %q: %w", ext, instErr)
			}
			if _, loadErr2 := db.Exec("LOAD " + ext); loadErr2 != nil {
				_ = db.Close()
				return nil, fmt.Errorf("failed to load DuckDB extension %q after install: %w", ext, loadErr2)
			}
		}
		slog.Info("DuckDB extension ready", "ext", ext)
	}

	duckDBSingleton = db
	slog.Info("DuckDB singleton initialized — extensions loaded once for process lifetime")
	return duckDBSingleton, nil
}

// ─────────────────────────────────────────────────────────────────────────────

// prepareIcebergQuery resolves the IST time range, initializes the shared
// DuckDB connection, locks duckDBMu, configures S3, and builds the
// iceberg_scan source — the setup shared by every historical query
// (RunHistoricalAnalysis, RunHistoricalBreakdown). The mutex is held on
// return; callers MUST defer the returned unlock() for the entire duration of
// their query execution (matching the existing "hold duckDBMu for the whole
// S3-config + query sequence" invariant), even though each caller only runs
// its own query shape against the shared source.
func prepareIcebergQuery(ctx context.Context, startTS, endTS string) (db *sql.DB, icebergSource string, startStr string, endStr string, unlock func(), err error) {
	parseIST := func(ts string) (time.Time, error) {
		formatted := strings.Replace(ts, "T", " ", 1)
		if len(formatted) == 16 {
			formatted += ":00"
		}
		return time.ParseInLocation("2006-01-02 15:04:05", formatted, istZone)
	}

	var endDt, startDt time.Time
	if endTS != "" {
		endDt, err = parseIST(endTS)
		if err != nil {
			return nil, "", "", "", nil, fmt.Errorf("invalid end_ts format: %w", err)
		}
	} else {
		endDt = time.Now().In(istZone)
	}
	if startTS != "" {
		startDt, err = parseIST(startTS)
		if err != nil {
			return nil, "", "", "", nil, fmt.Errorf("invalid start_ts format: %w", err)
		}
	} else {
		startDt = endDt.Add(-7 * 24 * time.Hour)
	}

	db, err = initDuckDB()
	if err != nil {
		return nil, "", "", "", nil, fmt.Errorf("DuckDB initialization failed: %w", err)
	}

	// Hold the mutex for the entire S3-config + query sequence.
	// initDuckDB released it above; we re-acquire here to serialize requests.
	duckDBMu.Lock()
	unlock = func() { duckDBMu.Unlock() }

	s3Endpoint := strings.TrimPrefix(strings.TrimPrefix(config.S3Endpoint, "http://"), "https://")
	safeAccessKey := strings.ReplaceAll(config.S3AccessKey, "'", "''")
	safeSecretKey := strings.ReplaceAll(config.S3SecretKey, "'", "''")

	useSSL := "true"
	if strings.HasPrefix(config.S3Endpoint, "http://") {
		useSSL = "false"
	}

	s3Stmts := []string{
		fmt.Sprintf("SET s3_endpoint='%s'", s3Endpoint),
		fmt.Sprintf("SET s3_access_key_id='%s'", safeAccessKey),
		fmt.Sprintf("SET s3_secret_access_key='%s'", safeSecretKey),
		"SET s3_url_style='path'",
		fmt.Sprintf("SET s3_use_ssl=%s", useSSL),
	}
	for _, stmt := range s3Stmts {
		if _, execErr := db.ExecContext(ctx, stmt); execErr != nil {
			unlock()
			return nil, "", "", "", nil, fmt.Errorf("failed to set S3 config") // DO NOT leak stmt in error
		}
	}

	basePath := strings.TrimRight(config.IcebergS3Path, "/")
	slog.Info("Scanning Iceberg table via native reader", "table_path", basePath)

	// Read via DuckDB's native Iceberg reader against the table's current
	// snapshot (manifests + delete files) instead of globbing the /data/
	// folder directly. A raw Parquet glob bypasses Iceberg's metadata layer
	// entirely: no manifest-level partition/file pruning, no snapshot
	// isolation (it can pick up orphaned or pre-compaction files still
	// sitting under /data/ before GC runs), and no way to honor real delete
	// files. allow_moved_paths handles the daily Spark compaction rewriting
	// file locations underneath us.
	// The QUALIFY dedup is kept as a defensive fallback on top of
	// iceberg_scan's own delete handling until that's validated against
	// production data — safe to remove once confirmed redundant.
	icebergSource = fmt.Sprintf(`(
        SELECT * FROM iceberg_scan('%s', allow_moved_paths => true)
        QUALIFY row_number() OVER (PARTITION BY event_timestamp, authCode ORDER BY input_kafka_timestamp DESC NULLS LAST) = 1
    ) AS stream_data`, basePath)

	startStr = startDt.UTC().Format("2006-01-02 15:04:05")
	endStr = endDt.UTC().Format("2006-01-02 15:04:05")
	return db, icebergSource, startStr, endStr, unlock, nil
}

// RunHistoricalAnalysis executes a historical analysis using DuckDB on Iceberg data.
// ctx is the request context — cancellation aborts the in-flight DuckDB query and
// releases the global duckDBMu lock so subsequent requests are not starved.
func RunHistoricalAnalysis(ctx context.Context, ruleDict map[string]interface{}, startTS, endTS string) ([]map[string]interface{}, error) {
	db, icebergSource, startStr, endStr, unlock, err := prepareIcebergQuery(ctx, startTS, endTS)
	if err != nil {
		return nil, err
	}
	defer unlock()

	// Build the query dynamically based on the rule
	grouping, _ := ruleDict["grouping"].(map[string]interface{})
	groupingKeys, _ := grouping["keys"].([]interface{})

	isGlobal := len(groupingKeys) == 1 && fmt.Sprintf("%v", groupingKeys[0]) == "__GLOBAL__"

	var selectKeys, groupByClause string

	if isGlobal {
		groupByClause = "GROUP BY window_start"
		selectKeys = "'__GLOBAL__' as groupKey"
	} else {
		var flatKeys []string
		for _, k := range groupingKeys {
			ks := fmt.Sprintf("%v", k)
			translated := TranslateField(ks)
			validated, err := ValidateIdentifier(translated)
			if err != nil {
				return nil, fmt.Errorf("invalid grouping key %q: %w", ks, err)
			}
			flatKeys = append(flatKeys, validated)
		}

		if len(flatKeys) == 1 {
			selectKeys = fmt.Sprintf("COALESCE(CAST(%s AS VARCHAR), 'N/A') as groupKey", flatKeys[0])
		} else {
			var parts []string
			for _, k := range flatKeys {
				parts = append(parts, fmt.Sprintf("COALESCE(CAST(%s AS VARCHAR), 'N/A')", k))
			}
			selectKeys = strings.Join(parts, " || '|' || ") + " as groupKey"
		}
		groupByClause = "GROUP BY window_start, " + strings.Join(flatKeys, ", ")
	}

	// Aggregations
	aggs, _ := ruleDict["aggregations"].([]interface{})
	var aggSelects []string
	var aggAliases = make(map[string]bool)

	for _, a := range aggs {
		agg, ok := a.(map[string]interface{})
		if !ok {
			continue
		}
		fieldRaw, _ := agg["field"].(string)
		aliasRaw, _ := agg["alias"].(string)
		funcRaw, _ := agg["function"].(string)

		translated := TranslateField(fieldRaw)
		field, err := ValidateIdentifier(translated)
		if err != nil {
			return nil, fmt.Errorf("invalid aggregation field %q: %w", fieldRaw, err)
		}
		alias, err := ValidateIdentifier(aliasRaw)
		if err != nil {
			return nil, fmt.Errorf("invalid aggregation alias %q: %w", aliasRaw, err)
		}
		aggAliases[alias] = true

		funcUpper := strings.ToUpper(funcRaw)
		switch funcUpper {
		case "COUNT_DISTINCT":
			aggSelects = append(aggSelects, fmt.Sprintf("COUNT(DISTINCT %s) as %s", field, alias))
		case "COUNT":
			aggSelects = append(aggSelects, fmt.Sprintf("COUNT(%s) as %s", field, alias))
		case "SUM", "AVG", "MIN", "MAX":
			aggSelects = append(aggSelects, fmt.Sprintf("%s(%s) as %s", funcUpper, field, alias))
		default:
			slog.Warn("Unknown aggregation function, defaulting to COUNT", "function", funcRaw)
			aggSelects = append(aggSelects, fmt.Sprintf("COUNT(%s) as %s", field, alias))
		}
	}

	aggClause := strings.Join(aggSelects, ", ")

	// Historical analysis always windows and range-filters on the Iceberg
	// `event_timestamp` column — the canonical ingestion-time field written
	// by the Flink job. A rule's windowing.time_type / windowing.timestamp_field
	// (e.g. reqDateTime, pidTs) only steers the live streaming engine's clock;
	// honoring them here too let historical queries silently window/filter on
	// sparse or inconsistent payload fields instead, producing wrong results
	// for any rule using a custom timestamp source.
	const windowField = "event_timestamp"
	windowing, _ := ruleDict["windowing"].(map[string]interface{})

	// Window size
	sizeMs := int64(300000)
	if windowing != nil {
		if sm, ok := windowing["size_ms"].(float64); ok {
			sizeMs = int64(sm)
		}
	}
	sizeSeconds := sizeMs / 1000
	if sizeSeconds < 1 {
		sizeSeconds = 1
	}

	// Filters
	var filterAST map[string]interface{}
	if f, ok := ruleDict["filters"].(map[string]interface{}); ok {
		filterAST = f
	}

	parsedWhere, filterParams := ParseFilterNode(filterAST)
	whereClause := ""
	if parsedWhere != "" {
		whereClause = "AND " + parsedWhere
	}

	// Having thresholds
	havingThresholds, _ := ruleDict["having_thresholds"].(map[string]interface{})
	havingExpr, _ := havingThresholds["expression"].(string)

	parsedHaving, err := ParseHavingExpression(havingExpr, aggAliases)
	if err != nil {
		slog.Warn("Invalid HAVING expression, ignoring", "error", err)
		parsedHaving = ""
	}

	var thresholdMetCol string
	if parsedHaving != "" {
		thresholdMetCol = fmt.Sprintf(", CASE WHEN (%s) THEN true ELSE false END as threshold_met", parsedHaving)
	} else {
		thresholdMetCol = ", false as threshold_met"
	}

	// startStr/endStr (already UTC — see prepareIcebergQuery) are what gets
	// bound to DuckDB's try_cast(? AS TIMESTAMP); the Iceberg event_timestamp
	// column is also stored in UTC, so this is an apples-to-apples compare.
	query := fmt.Sprintf(`
        SELECT
            time_bucket(INTERVAL '%d seconds', try_cast(%s AS TIMESTAMP)) as window_start,
            %s,
            %s
            %s
        FROM %s
        WHERE try_cast(%s AS TIMESTAMP) >= try_cast(? AS TIMESTAMP)
        AND try_cast(%s AS TIMESTAMP) <= try_cast(? AS TIMESTAMP)
        %s
        %s
        ORDER BY window_start DESC
        LIMIT 5000
        `, sizeSeconds, windowField, selectKeys, aggClause, thresholdMetCol, icebergSource,
        windowField, windowField, whereClause, groupByClause,
    )

	// Build params: time range first, then filter params
	allParams := make([]interface{}, 0, 2+len(filterParams))
	allParams = append(allParams, startStr, endStr)
	allParams = append(allParams, filterParams...)

	slog.Info("Executing DuckDB historical analysis",
		"start_utc", startStr,
		"end_utc", endStr,
		"window_seconds", sizeSeconds,
	)
	slog.Debug("DuckDB query", "query", query, "params", allParams)

	// Use QueryContext so that if the HTTP client disconnects (AbortController on
	// frontend), the context is cancelled and DuckDB releases the mutex promptly.
	rows, err := db.QueryContext(ctx, query, allParams...)
	if err != nil {
		slog.Error("DuckDB query failed", "error", err)
		return nil, fmt.Errorf("DuckDB query failed: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get DuckDB columns: %w", err)
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			slog.Error("Failed to scan DuckDB row", "error", err)
			continue
		}

		row := make(map[string]interface{}, len(columns))
		for i, col := range columns {
			val := values[i]
			switch v := val.(type) {
			case time.Time:
				// Format in IST so frontend always sees consistent IST timestamps
				row[col] = v.In(istZone).Format("2006-01-02 15:04:05")
			case *time.Time:
				if v != nil {
					row[col] = v.In(istZone).Format("2006-01-02 15:04:05")
				} else {
					row[col] = nil
				}
			default:
				row[col] = val
			}
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		slog.Error("DuckDB rows iteration error", "error", err)
		return nil, fmt.Errorf("DuckDB rows iteration error: %w", err)
	}

	slog.Info("DuckDB historical analysis complete", "result_rows", len(results))
	return results, nil
}

// modalityBreakdownCols maps a display label to its Iceberg flag column.
// Each column is a 0/1 DoubleType per the ingestion schema, so SUM() over the
// matched rows gives a usage count for that auth modality.
var modalityBreakdownCols = []struct{ Label, Col string }{
	{"OTP", "otpusedflag"},
	{"PIN", "pinusedflag"},
	{"Biometric — Fingerprint", "btfmrusedflag"},
	{"Biometric — Iris", "btiirusedflag"},
	{"Face", "faceused"},
	{"Demographic", "piusedflag"},
}

// RunHistoricalBreakdown computes forensic drill-down breakdowns (modality
// mix, auth outcome, geographic hotspot, fingerprint match-score histogram)
// over the exact same matched rows RunHistoricalAnalysis would use — same
// iceberg_scan source, time range, and rule filter — but summarized across
// the whole range instead of windowed. This is a drill-down feature used
// occasionally, not a hot path, so it deliberately runs each breakdown as its
// own query against the shared source rather than one hand-rolled multi-branch
// UNION ALL: that would need every branch's ? placeholders kept in lockstep,
// and a silent ordering mistake there would produce wrong breakdown numbers
// with no visible error — a correctness risk not worth the saved DuckDB scans.
func RunHistoricalBreakdown(ctx context.Context, ruleDict map[string]interface{}, startTS, endTS string) (map[string]interface{}, error) {
	db, icebergSource, startStr, endStr, unlock, err := prepareIcebergQuery(ctx, startTS, endTS)
	if err != nil {
		return nil, err
	}
	defer unlock()

	var filterAST map[string]interface{}
	if f, ok := ruleDict["filters"].(map[string]interface{}); ok {
		filterAST = f
	}
	parsedWhere, filterParams := ParseFilterNode(filterAST)
	whereClause := ""
	if parsedWhere != "" {
		whereClause = "AND " + parsedWhere
	}

	baseWhere := fmt.Sprintf(`
		WHERE try_cast(event_timestamp AS TIMESTAMP) >= try_cast(? AS TIMESTAMP)
		AND try_cast(event_timestamp AS TIMESTAMP) <= try_cast(? AS TIMESTAMP)
		%s`, whereClause)
	baseParams := append([]interface{}{startStr, endStr}, filterParams...)

	// runCategoryBreakdown runs a "label -> count" GROUP BY query and returns
	// it as a list of {label, count} maps for a uniform frontend shape.
	runCategoryBreakdown := func(labelExpr, extraWhere, orderLimit string) ([]map[string]interface{}, error) {
		query := fmt.Sprintf(`SELECT %s AS label, COUNT(*) AS count FROM %s %s %s GROUP BY 1 %s`,
			labelExpr, icebergSource, baseWhere, extraWhere, orderLimit)
		rows, qErr := db.QueryContext(ctx, query, baseParams...)
		if qErr != nil {
			return nil, qErr
		}
		defer rows.Close()
		var out []map[string]interface{}
		for rows.Next() {
			var label string
			var count int64
			if scanErr := rows.Scan(&label, &count); scanErr != nil {
				return nil, scanErr
			}
			out = append(out, map[string]interface{}{"label": label, "count": count})
		}
		return out, rows.Err()
	}

	result := make(map[string]interface{})

	// ── Modality mix: one query, one row, one SUM column per modality ───────
	var modalitySelects []string
	for _, m := range modalityBreakdownCols {
		modalitySelects = append(modalitySelects, fmt.Sprintf("CAST(COALESCE(SUM(%s),0) AS BIGINT) AS %s", m.Col, m.Col))
	}
	modalityQuery := fmt.Sprintf(`SELECT %s FROM %s %s`, strings.Join(modalitySelects, ", "), icebergSource, baseWhere)
	modalityRows, err := db.QueryContext(ctx, modalityQuery, baseParams...)
	if err != nil {
		return nil, fmt.Errorf("modality breakdown query failed: %w", err)
	}
	// Explicit Close() (not deferred) — MaxOpenConns(1) means this rows set
	// must release the connection before the next QueryContext call below, or
	// that call would block waiting for a connection that never frees up.
	var modalityMix []map[string]interface{}
	if modalityRows.Next() {
		vals := make([]interface{}, len(modalityBreakdownCols))
		ptrs := make([]interface{}, len(modalityBreakdownCols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if scanErr := modalityRows.Scan(ptrs...); scanErr != nil {
			modalityRows.Close()
			return nil, fmt.Errorf("modality breakdown scan failed: %w", scanErr)
		}
		for i, m := range modalityBreakdownCols {
			count, _ := vals[i].(int64)
			modalityMix = append(modalityMix, map[string]interface{}{"label": m.Label, "count": count})
		}
	}
	modalityRows.Close()
	result["modality_mix"] = modalityMix

	// ── Auth outcome (authresult) ────────────────────────────────────────────
	authOutcome, err := runCategoryBreakdown("COALESCE(authresult, 'UNKNOWN')", "", "ORDER BY count DESC LIMIT 10")
	if err != nil {
		return nil, fmt.Errorf("auth outcome breakdown query failed: %w", err)
	}
	result["auth_outcome"] = authOutcome

	// ── Geographic hotspot (top 10 states by matched-event volume) ──────────
	geoHotspot, err := runCategoryBreakdown("COALESCE(locationstatecode, 'UNKNOWN')", "", "ORDER BY count DESC LIMIT 10")
	if err != nil {
		return nil, fmt.Errorf("geo hotspot breakdown query failed: %w", err)
	}
	result["geo_hotspot"] = geoHotspot

	// ── Fingerprint match-score histogram (10-point buckets) ─────────────────
	// Fingerprint is used as the representative biometric score since it's
	// the most common modality in Aadhaar auth traffic — not a universal
	// score across every rule.
	scoreQuery := fmt.Sprintf(`
		SELECT CAST(bucket_start AS VARCHAR) || '-' || CAST(bucket_start + 10 AS VARCHAR) AS label, COUNT(*) AS count
		FROM (
			SELECT FLOOR(fingermatchscore / 10) * 10 AS bucket_start
			FROM %s
			%s
			AND fingermatchscore IS NOT NULL
		) t
		GROUP BY bucket_start
		ORDER BY bucket_start`, icebergSource, baseWhere)
	scoreRows, err := db.QueryContext(ctx, scoreQuery, baseParams...)
	if err != nil {
		return nil, fmt.Errorf("match-score histogram query failed: %w", err)
	}
	defer scoreRows.Close() // last query in this function — safe to close on return
	var scoreHistogram []map[string]interface{}
	for scoreRows.Next() {
		var label string
		var count int64
		if scanErr := scoreRows.Scan(&label, &count); scanErr != nil {
			return nil, fmt.Errorf("match-score histogram scan failed: %w", scanErr)
		}
		scoreHistogram = append(scoreHistogram, map[string]interface{}{"label": label, "count": count})
	}
	if err := scoreRows.Err(); err != nil {
		return nil, fmt.Errorf("match-score histogram rows error: %w", err)
	}
	result["match_score_histogram"] = scoreHistogram

	slog.Info("DuckDB historical breakdown complete",
		"modality_rows", len(modalityMix),
		"auth_outcome_rows", len(authOutcome),
		"geo_hotspot_rows", len(geoHotspot),
		"score_histogram_rows", len(scoreHistogram),
	)
	return result, nil
}
