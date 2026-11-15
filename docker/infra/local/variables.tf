variable "region" {
  type    = string
  default = "eu-west-1"
}

variable "profile" {
  type    = string
  default = "localdev"
}

variable "localstack_endpoint" {
  type    = string
  default = "http://localhost:4566"
}

variable "web_origins" {
  description = "Browser origins allowed to upload import files to the store products bucket (oms-web dev server)"
  type        = list(string)
  default     = ["http://localhost:5173", "http://127.0.0.1:5173"]
}

variable "import_upload_retention_days" {
  description = "Days before an uploaded import file (tag purpose=import-upload) is deleted (BR-CAT-17)"
  type        = number
  default     = 7
}
