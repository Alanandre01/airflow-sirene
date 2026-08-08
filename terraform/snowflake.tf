resource "snowflake_warehouse" "alan_wh" {
  name           = "ALAN_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true

  warehouse_type                      = "STANDARD"
  scaling_policy                      = "STANDARD"
  min_cluster_count                   = 1
  max_cluster_count                   = 1
  enable_query_acceleration           = false
  query_acceleration_max_scale_factor = 8
}
