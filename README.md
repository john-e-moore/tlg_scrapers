Webscraping and monitoring for economic data.

'historical' contains scripts for ingesting historical data into S3.
'jobs' contains code for recurring Lambda functions that download data and send emails when a website is updated.

Eventual lambda function flow:
- check_for_updates
- if updates
 - ingest
 - process
 - aggregate
 - update charts
 - email spreadsheet + charts
