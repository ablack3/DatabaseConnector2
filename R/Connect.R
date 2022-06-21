# @file Connect.R
#
# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of DatabaseConnector
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

checkIfDbmsIsSupported <- function(dbms) {
  supportedDatabases <- c(
    "sql server",
    # "oracle",
    "postgresql",
    # "pdw",
    "redshift"#,
    # "impala",
    # "bigquery",
    # "netezza",
    # "sqlite",
    # "spark"
  )
  if (!dbms %in% supportedDatabases) {
    abort(sprintf(
      "DBMS '%s' not supported. Please use one of these values: '%s'",
      dbms,
      paste(supportedDatabases, collapse = "', '")
    ))
  }
}



#' Store information needed to connection to a DBMS
#'
#' @param drv an object that inherits from DBIDriver, or an existing DBIConnection object.
#' @param ... authentication arguments needed by the DBMS instance; these typically include user, password, host, port, dbname, etc. For details see the appropriate DBIDriver.
#'
#' @return
#' @export
#'
#' @examples
dbConnectDetails <- function(drv, ...) {
  result <- list(drv = drv, ...)
  class(result) <- c("dbConnectDetails")
  result
}

#' @export
connect <- function(connectionDetails) UseMethod("connect")

#' @export
#' @importFrom DBI dbConnect
#' @importFrom rlang !!!
connect.dbConnectDetails <- function(connectionDetails) {
  # do.call("dbConnect", connectionDetails)
  conn <- rlang::inject(DBI::dbConnect(!!!connectionDetails))
  attr(conn, "dbms") <- dbms(conn)
  conn
}

#' @export
disconnect <-  DBI::dbDisconnect

dbms <- function(connection) {
  switch (class(connection),
    'case' = 'sql server',
    'case' = 'posrgresql',
    'case' = 'redshift'
  )
}



