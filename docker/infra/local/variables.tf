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