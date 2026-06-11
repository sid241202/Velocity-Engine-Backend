package services

import (
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"
	"unicode"

	"velocity-engine-control-plane-backend-go/internal/config"

	_ "github.com/marcboeker/go-duckdb"
)

var icebergS3PathRegex = regexp.MustCompile(`^s3://[a-zA-Z0-9._/-]+$`)

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
		}
	}

	return "", nil
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

// RunHistoricalAnalysis executes a historical analysis using DuckDB on Iceberg data.
// This replicates the Python duckdb_worker.run_historical_analysis function exactly.
func RunHistoricalAnalysis(ruleDict map[string]interface{}, startTS, endTS string) ([]map[string]interface{}, error) {
	// IST timezone: UTC+5:30
	ist := time.FixedZone("IST", 5*60*60+30*60)

	// Resolve time bounds
	var endDt, startDt time.Time

	if endTS != "" {
		parsed, err := time.Parse("2006-01-02T15:04:05", endTS)
		if err != nil {
			// Try alternate format
			parsed, err = time.Parse("2006-01-02 15:04:05", endTS)
			if err != nil {
				return nil, fmt.Errorf("invalid end_ts format: %w", err)
			}
		}
		endDt = parsed
	} else {
		endDt = time.Now().In(ist)
		// Strip timezone info to match Python behavior
		endDt = time.Date(endDt.Year(), endDt.Month(), endDt.Day(),
			endDt.Hour(), endDt.Minute(), endDt.Second(), 0, time.UTC)
	}

	if startTS != "" {
		parsed, err := time.Parse("2006-01-02T15:04:05", startTS)
		if err != nil {
			parsed, err = time.Parse("2006-01-02 15:04:05", startTS)
			if err != nil {
				return nil, fmt.Errorf("invalid start_ts format: %w", err)
			}
		}
		startDt = parsed
	} else {
		startDt = endDt.Add(-7 * 24 * time.Hour)
	}

	// Open in-memory DuckDB connection
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	// Install and load extensions
	for _, stmt := range []string{
		"INSTALL iceberg",
		"LOAD iceberg",
		"INSTALL httpfs",
		"LOAD httpfs",
	} {
		if _, err := db.Exec(stmt); err != nil {
			return nil, fmt.Errorf("failed to execute '%s': %w", stmt, err)
		}
	}

	// Configure S3
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
		if _, err := db.Exec(stmt); err != nil {
			return nil, fmt.Errorf("failed to set S3 config") // DO NOT leak stmt in error
		}
	}

	// Build the Iceberg scan source expression
	icebergSource := fmt.Sprintf("iceberg_scan('%s')", config.IcebergS3Path)

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

	// Window field
	windowField := "event_timestamp"
	windowing, _ := ruleDict["windowing"].(map[string]interface{})
	if windowing != nil {
		timeType, _ := windowing["time_type"].(string)
		useKafkaTS, _ := windowing["use_kafka_timestamp"].(bool)

		if timeType == "PROCESSING_TIME" || useKafkaTS {
			windowField = "event_timestamp"
		} else {
			uiTimeField, _ := windowing["timestamp_field"].(string)
			if uiTimeField != "" && uiTimeField != "_event_timestamp_epoch_ms" {
				translated := TranslateField(uiTimeField)
				validated, err := ValidateIdentifier(translated)
				if err == nil {
					windowField = validated
				}
			}
		}
	}

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

	startStr := startDt.Format("2006-01-02 15:04:05")
	endStr := endDt.Format("2006-01-02 15:04:05")

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
    LIMIT 1000
    `, sizeSeconds, windowField,
		selectKeys,
		aggClause,
		thresholdMetCol,
		icebergSource,
		windowField,
		windowField,
		whereClause,
		groupByClause,
	)

	// Build params: time range first, then filter params
	allParams := make([]interface{}, 0, 2+len(filterParams))
	allParams = append(allParams, startStr, endStr)
	allParams = append(allParams, filterParams...)

	slog.Info("Executing DuckDB historical analysis",
		"start", startStr,
		"end", endStr,
		"window_seconds", sizeSeconds,
	)
	slog.Debug("DuckDB query", "query", query, "params", allParams)

	rows, err := db.Query(query, allParams...)
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
				row[col] = v.Format("2006-01-02 15:04:05")
			case *time.Time:
				if v != nil {
					row[col] = v.Format("2006-01-02 15:04:05")
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
