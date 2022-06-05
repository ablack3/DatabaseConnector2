test_that("dbConnectionDetails work", {
  connectionDetails <- dbConnectDetails(RSQLite::SQLite(), ":memory:")
  con <- connect(connectionDetails)
  expect_true(DBI::dbIsValid(con))
  DBI::dbDisconnect(con)
})
