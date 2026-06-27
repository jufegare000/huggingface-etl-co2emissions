resource "aws_secretsmanager_secret" "this" {
  name = "${var.project_name}-${var.secret_name}-${var.environment}-v2"
}

resource "aws_secretsmanager_secret_version" "this" {
  secret_id     = aws_secretsmanager_secret.this.id
  secret_string = var.secret_value
}