package services

// FinancialAuaCodes is the hardcoded set of AUA codes UIDAI treats as
// "financial" (banks and similar institutions) — ported from the legacy
// kafka-auth-txn-fraud-alert-job's FinancialAuaList.txt. This lives here,
// not as a per-rule value an analyst types into the UI, so the
// IS_FINANCIAL_AUA filter operator (VisualFilterBuilder.jsx) has a single
// server-side source of truth instead of every rule carrying its own copy
// of a 131-code list. Kept in sync with the identical list hardcoded in the
// Flink job (in.gov.uidai.dp.velocity.engine.utils.FinancialAuaList),
// which is what actually enforces this against live traffic — this copy
// only needs to match for the backend's Historical Test/Analysis (DuckDB)
// path to agree with live results.
var FinancialAuaCodes = []string{
	"0003520000", "0000980000", "0003410000", "0001500000", "0003370000",
	"0008500000", "0003200000", "0009600000", "0006500000", "0001060000",
	"0000050000", "0023000200", "0002670000", "0000180000", "0005700000",
	"0000320000", "0007700000", "0000810000", "0002740000", "0007500000",
	"0001260000", "0002800000", "0005800000", "0002570000", "0000700000",
	"0002880000", "0006400000", "0001590000", "0000420000", "0002400000",
	"0003360000", "0001450000", "0000230000", "0009800000", "0001800000",
	"0002100000", "0000600000", "0004000000", "0000470000", "0009400000",
	"0007800000", "0000550000", "0002000000", "0007200000", "0002890000",
	"0000760000", "0006200000", "0006900000", "0000110000", "0007100000",
	"0002600000", "0003340000", "0011000000", "0006100000", "0005200000",
	"0001700000", "0000580000", "0000770000", "0004300000", "0005400000",
	"0003100000", "0000410000", "0000070000", "0000590000", "0008700000",
	"0000250000", "0043000100", "0001900000", "0000080000", "0001400000",
	"0001940000", "0000310000", "0003010000", "0002530000", "0001080000",
	"0000920000", "0003170000", "0002900000", "0000190000", "0008800000",
	"0003600000", "0000120000", "0001100000", "0000170000", "0001200000",
	"0002300000", "0001600000", "0000040000", "0023000100", "0000220000",
	"0005500000", "0003070000", "0000060000", "0005600000", "0004900000",
	"0000280000", "0000200000", "0000680000", "0000620000", "0003250000",
	"0002500000", "0000630000", "0001110000", "0002260000", "0003320000",
	"0003210000", "0003180000", "0003190000", "0003090000", "0003120000",
	"0003220000", "0001160000", "0003420000", "0003450000", "0003470000",
	"0003550000", "0003560000", "0003570000", "0003580000", "0003610000",
	"0003670000", "0003690000", "0001780000", "0003810000", "0004010000",
	"0004040000", "0004160000", "0004180000", "0004250000", "0004270000",
	"0004310000",
}

// financialAuaCodesAsAny returns FinancialAuaCodes as []interface{}, the
// shape database/sql's variadic query args need.
func financialAuaCodesAsAny() []interface{} {
	out := make([]interface{}, len(FinancialAuaCodes))
	for i, c := range FinancialAuaCodes {
		out[i] = c
	}
	return out
}
