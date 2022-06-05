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
    "oracle",
    "postgresql",
    "pdw",
    "redshift",
    "impala",
    "bigquery",
    "netezza",
    "sqlite",
    "spark"
  )
  if (!dbms %in% supportedDatabases) {
    abort(sprintf(
      "DBMS '%s' not supported. Please use one of these values: '%s'",
      dbms,
      paste(supportedDatabases, collapse = "', '")
    ))
  }
}



#' Store the information to connect to a database
#'
#' \code{createConnectionDetails} creates a list containing all details needed to connect to a
#' database. There are three ways to call this function:
#' \itemize{
#'   \item \code{createConnectionDetails(dbms, user, password, server, port, extraSettings, oracleDriver, pathToDriver)}
#'   \item \code{createConnectionDetails(dbms, connectionString, pathToDriver)}
#'   \item \code{createConnectionDetails(dbms, connectionString, user, password, pathToDriver)}
#' }
#'
#' @details
#' This function creates an object that contains all information needed to connect to a database.
#' The connectionDetails object can then be used in the \code{\link{connect}} function.
#'
#' @return
#' A list with all the details needed to connect to a database.
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(
#'   dbms = "postgresql",
#'   server = "localhost/postgres",
#'   user = "root",
#'   password = "blah"
#' )
#' conn <- connect(connectionDetails)
#' dbGetQuery(conn, "SELECT COUNT(*) FROM person")
#' disconnect(conn)
#' }
#' @export
#' @importFrom lifecycle deprecated
createConnectionDetails <- function(dbms,
                                    user = NULL,
                                    password = NULL,
                                    server = NULL,
                                    port = NULL,
                                    extraSettings = NULL,
                                    oracleDriver = deprecated(),
                                    connectionString = NULL,
                                    pathToDriver = Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")) {
  checkIfDbmsIsSupported(dbms)
  pathToDriver <- path.expand(pathToDriver)
  checkPathToDriver(pathToDriver, dbms)

  result <- list(
    dbms = dbms,
    extraSettings = extraSettings,
    oracleDriver = oracleDriver,
    pathToDriver = pathToDriver
  )

  userExpression <- rlang::enquo(user)
  result$user <- function() rlang::eval_tidy(userExpression)

  passWordExpression <- rlang::enquo(password)
  result$password <- function() rlang::eval_tidy(passWordExpression)

  serverExpression <- rlang::enquo(server)
  result$server <- function() rlang::eval_tidy(serverExpression)

  portExpression <- rlang::enquo(port)
  result$port <- function() rlang::eval_tidy(portExpression)

  csExpression <- rlang::enquo(connectionString)
  result$connectionString <- function() rlang::eval_tidy(csExpression)

  class(result) <- "connectionDetails"
  return(result)
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
  attr(conn, "dbms") <- class(conn)
  conn
}

