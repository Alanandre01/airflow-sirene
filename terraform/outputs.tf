output "s3_bucket_id" {
  description = "Identifiant du bucket S3 du data lake"
  value       = aws_s3_bucket.alan_data_lake_fr.id
}

output "s3_bucket_arn" {
  description = "ARN du bucket S3 du data lake"
  value       = aws_s3_bucket.alan_data_lake_fr.arn
}

output "snowflake_warehouse_name" {
  description = "Nom du warehouse Snowflake"
  value       = snowflake_warehouse.alan_wh.name
}

output "snowflake_warehouse_size" {
  description = "Taille du warehouse Snowflake"
  value       = snowflake_warehouse.alan_wh.warehouse_size
}
