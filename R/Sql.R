#' Execute SQL code
#'
#' @description
#' This function executes SQL consisting of one or more statements.
#'
#' @param connection          The connection to the database server.
#' @param sql                 The SQL to be executed
#' @param profile             When true, each separate statement is written to file prior to sending to
#'                            the server, and the time taken to execute a statement is displayed.
#' @param progressBar         When true, a progress bar is shown based on the statements in the SQL
#'                            code.
#' @param reportOverallTime   When true, the function will display the overall time taken to execute
#'                            all statements.
#' @param errorReportFile     The file where an error report will be written if an error occurs. Defaults to
#'                            'errorReportSql.txt' in the current working directory.
#' @param runAsBatch          When true the SQL statements are sent to the server as a single batch, and
#'                            executed there. This will be faster if you have many small SQL statements, but
#'                            there will be no progress bar, and no per-statement error messages. If the
#'                            database platform does not support batched updates the query is executed without
#'                            batching.
#'
#' @details
#' This function splits the SQL in separate statements and sends it to the server for execution. If an
#' error occurs during SQL execution, this error is written to a file to facilitate debugging.
#' Optionally, a progress bar is shown and the total time taken to execute the SQL is displayed.
#' Optionally, each separate SQL statement is written to file, and the execution time per statement is
#' shown to aid in detecting performance issues.
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(
#'   dbms = "postgresql",
#'   server = "localhost",
#'   user = "root",
#'   password = "blah",
#'   schema = "cdm_v4"
#' )
#' conn <- connect(connectionDetails)
#' executeSql(conn, "CREATE TABLE x (k INT); CREATE TABLE y (k INT);")
#' disconnect(conn)
#' }
#' @export
#' @importFrom rlang inform abort
executeSql <- function(connection,
                       sql,
                       profile = FALSE,
                       progressBar = FALSE,
                       reportOverallTime = FALSE,
                       errorReportFile = NULL) {

  if (!DBI::dbIsValid(connection)) abort("Connection is closed")

  startTime <- Sys.time()
  sqlStatements <- SqlRender::splitSql(sql)
  if (progressBar) pb <- txtProgressBar(style = 3)

  dbWithTransaction(connection, {
    for (i in seq_along(sqlStatements)) {
      sqlStatement <- sqlStatements[i]
      if (profile) readr::write_file(sqlStatement, paste0("statement_", i, ".sql"))

      tryCatch(
        {
          startQuery <- Sys.time()
          DBI::dbExecute(connection, sqlStatement)
          if (profile) {
            delta <- Sys.time() - startQuery
            inform(paste("Statement ", i, "took", delta, attr(delta, "units")))
          }
        },
        error = function(err) {
          if (is.character(errorReportFile)) {
            .createErrorReport(dbms(connection), err$message, sqlStatement, errorReportFile)
          } else {
            rlang::abort(paste0("Error executing SQL:\n", err$message))
          }
        }
      )

      if (progressBar) setTxtProgressBar(pb, i / length(sqlStatements))
    }
  })
  if (progressBar) close(pb)

  if (reportOverallTime) {
    delta <- Sys.time() - startTime
    inform(paste("Executing SQL took", signif(delta, 3), attr(delta, "units")))
  }
}



#' Retrieve data to a data.frame
#'
#' @description
#' This function sends SQL to the server, and returns the results.
#'
#' @param connection           The connection to the database server.
#' @param sql                  The SQL to be send.
#' @param errorReportFile      The file where an error report will be written if an error occurs. Defaults to
#'                             'errorReportSql.txt' in the current working directory.
#' @param snakeCaseToCamelCase If true, field names are assumed to use snake_case, and are converted to camelCase.
#' @param integerAsNumeric Logical: should 32-bit integers be converted to numeric (double) values? If FALSE
#'                          32-bit integers will be represented using R's native \code{Integer} class.
#' @param integer64AsNumeric   Logical: should 64-bit integers be converted to numeric (double) values? If FALSE
#'                             64-bit integers will be represented using \code{bit64::integer64}.
#'
#' @details
#' This function sends the SQL to the server and retrieves the results. If an error occurs during SQL
#' execution, this error is written to a file to facilitate debugging. Null values in the database are converted
#' to NA values in R.
#'
#' @return
#' A data frame.
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(
#'   dbms = "postgresql",
#'   server = "localhost",
#'   user = "root",
#'   password = "blah",
#'   schema = "cdm_v4"
#' )
#' conn <- connect(connectionDetails)
#' count <- querySql(conn, "SELECT COUNT(*) FROM person")
#' disconnect(conn)
#' }
#' @export
querySql <- function(connection,
                     sql,
                     errorReportFile = file.path(getwd(), "errorReportSql.txt")) {

  if (!DBI::dbIsValid(connection)) abort("Connection is closed")

  # Calling splitSql, because this will also strip trailing semicolons (which cause Oracle to crash).
  sqlStatements <- SqlRender::splitSql(sql)
  if (length(sqlStatements) > 1) {
    abort(paste(
      "A query that returns a result can only consist of one SQL statement, but",
      length(sqlStatements),
      "statements were found"
    ))
  }
  tryCatch(
    {
      result <- DBI::dbExecute(connection, statement = sqlStatements[[1]])
    },
    error = function(err) {
      if (is.character(errorReportFile)) {
        .createErrorReport(dbms(connection), err$message, sqlStatement, errorReportFile)
      } else {
        rlang::abort(paste0("Error executing SQL:\n", err$message))
      }
    }
  )
}

.createErrorReport <- function(dbms, message, sql, fileName) {
  si <- sessionInfo()
  report <- paste(
    c("DBMS:", dbms, "\nError:", message, "\nSQL:", sql, "\n",
      "R version:", si$R.version$version.string, "",
      "Platform:", si$R.version$platform, "",
      "Attached base packages:", paste("-", si$basePkgs), "",
      "Other attached packages:",
      purrr::map_chr(si$otherPkgs, ~paste0("- ", .$Package, " (", .$Version, ")"))
    ), collapse = "\n")
  readr::write_file(report, fileName)
  abort(paste("Error executing SQL:",
              message,
              paste("An error report has been created at ", fileName),
              sep = "\n"
  ), call. = FALSE)
}
