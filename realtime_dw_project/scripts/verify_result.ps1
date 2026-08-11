$ErrorActionPreference = "Stop"

$projectDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $projectDir

function Invoke-ClickHouseScalar {
    param([Parameter(Mandatory)][string]$Query)
    $value = docker exec rtdw_clickhouse clickhouse-client --password clickhouse --query $Query
    if ($LASTEXITCODE -ne 0) { throw "ClickHouse query failed: $Query" }
    return [long]$value.Trim()
}

$checks = @(
    @{ Label = "dwd_user_behavior"; Mode = "positive"; Query = "SELECT count() FROM dwd_user_behavior FINAL" },
    @{ Label = "ads_realtime_overview"; Mode = "positive"; Query = "SELECT count() FROM ads_realtime_overview FINAL" },
    @{ Label = "ads_product_rank"; Mode = "positive"; Query = "SELECT count() FROM ads_product_rank FINAL" },
    @{ Label = "ads_category_rank"; Mode = "positive"; Query = "SELECT count() FROM ads_category_rank FINAL" },
    @{ Label = "ads_channel_funnel"; Mode = "positive"; Query = "SELECT count() FROM ads_channel_funnel FINAL" },
    @{ Label = "empty event_id rows"; Mode = "zero"; Query = "SELECT count() FROM dwd_user_behavior FINAL WHERE event_id = ''" },
    @{ Label = "invalid event_type rows"; Mode = "zero"; Query = "SELECT count() FROM dwd_user_behavior FINAL WHERE event_type NOT IN ('view','cart','order','pay')" },
    @{ Label = "duplicate event_id keys"; Mode = "zero"; Query = "SELECT count() FROM (SELECT event_id FROM dwd_user_behavior FINAL GROUP BY event_id HAVING count() > 1)" },
    @{ Label = "windows where UV > PV"; Mode = "zero"; Query = "SELECT count() FROM ads_realtime_overview FINAL WHERE uv > pv" },
    @{ Label = "reversed funnel windows"; Mode = "zero"; Query = "SELECT count() FROM ads_channel_funnel FINAL WHERE cart_users > view_users OR order_users > cart_users OR pay_users > order_users" },
    @{ Label = "dwd_order_detail"; Mode = "positive"; Query = "SELECT count() FROM dwd_order_detail FINAL WHERE is_deleted = 0" },
    @{ Label = "dim_product_scd2"; Mode = "positive"; Query = "SELECT count() FROM dim_product_scd2 FINAL WHERE is_deleted = 0" },
    @{ Label = "ads_order_lifecycle"; Mode = "positive"; Query = "SELECT count() FROM ads_order_lifecycle FINAL WHERE is_deleted = 0" },
    @{ Label = "ads_order_daily"; Mode = "positive"; Query = "SELECT count() FROM ads_order_daily FINAL WHERE is_deleted = 0" },
    @{ Label = "duplicate detail keys"; Mode = "zero"; Query = "SELECT count() FROM (SELECT detail_id FROM dwd_order_detail FINAL WHERE is_deleted = 0 GROUP BY detail_id HAVING count() > 1)" },
    @{ Label = "invalid order status"; Mode = "zero"; Query = "SELECT count() FROM dwd_order_detail FINAL WHERE is_deleted = 0 AND order_status NOT IN ('CREATED','PAID','CANCELLED','REFUNDED')" },
    @{ Label = "paid order without payment"; Mode = "zero"; Query = "SELECT count() FROM dwd_order_detail FINAL WHERE is_deleted = 0 AND order_status IN ('PAID','REFUNDED') AND (payment_id IS NULL OR payment_status != 'SUCCESS')" },
    @{ Label = "invalid refund"; Mode = "zero"; Query = "SELECT count() FROM dwd_order_detail FINAL WHERE is_deleted = 0 AND order_status = 'REFUNDED' AND (refund_id IS NULL OR refund_status != 'SUCCESS' OR refund_amount <= 0)" },
    @{ Label = "historical price mismatch"; Mode = "zero"; Query = "SELECT count() FROM dwd_order_detail FINAL WHERE is_deleted = 0 AND unit_price != catalog_price" },
    @{ Label = "invalid SCD2 current versions"; Mode = "zero"; Query = "SELECT count() FROM (SELECT product_id FROM dim_product_scd2 FINAL WHERE is_deleted = 0 GROUP BY product_id HAVING countIf(is_current = 1) != 1)" },
    @{ Label = "invalid order lifecycle"; Mode = "zero"; Query = "SELECT count() FROM ads_order_lifecycle FINAL WHERE is_deleted = 0 AND (paid_orders > total_orders OR cancelled_orders > total_orders OR refunded_orders > paid_orders OR refund_amount > paid_amount)" }
)

$passed = 0
$failed = 0
Write-Host "`n=== Verification Results ==="
foreach ($check in $checks) {
    $value = Invoke-ClickHouseScalar -Query $check.Query
    $ok = if ($check.Mode -eq "positive") { $value -gt 0 } else { $value -eq 0 }
    if ($ok) {
        Write-Host "  PASS: $($check.Label) = $value"
        $passed++
    } else {
        Write-Host "  FAIL: $($check.Label) = $value"
        $failed++
    }
}

Write-Host "verify summary: $passed PASS, $failed FAIL"
if ($failed -gt 0) { throw "$failed acceptance checks failed." }
