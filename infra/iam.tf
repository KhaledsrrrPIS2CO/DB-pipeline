# ── IAM Role for Glue ────────────────────────────────────────────────────────
# Glue needs permission to read Bronze and write Silver.
# Least privilege — nothing more than what Glue actually needs.

resource "aws_iam_role" "glue" {
  name = "db-pipeline-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Allow Glue to read/write S3
resource "aws_iam_role_policy" "glue_s3" {
  name = "db-pipeline-glue-s3"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*"
        ]
      }
    ]
  })
}

# Allow Glue to write CloudWatch logs
resource "aws_iam_role_policy_attachment" "glue_cloudwatch" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}