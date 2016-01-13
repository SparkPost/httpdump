# httpdump

Store HTTP request data, and provide a simple framework for post-processing in parallel.

### Example code

The program in the `output/loggly` directory is provided as an example implementation.

### Command-line parameters

The example program accepts two command line parameters and starts up an HTTP server on the specified port.

**-port** (default 80) listen for http requests on this port  
**-batch-interval** (default 10) how often to process stored requests, in seconds  

### Environment variables

**LOGGLY_TOKEN**  
Your Loggly API key. This uuid tells Loggly what account should store the incoming data. This is required, and there is no default.

**POSTGRESQL_DB**  
Database within local PostgreSQL instance where event data will be stored. Defaults to the database named after your user, which may not have been automatically created.

**POSTGRESQL_SCHEMA**  
Schema within local PostgreSQL database where event data will be stored. Defaults to `request_dump`, and will be automatically created if it doesn't exist.

**POSTGRESQL_USER**  
User to connect to PostgreSQL as. This defaults to the OS-level username, which will usually have had an account automatically created.

**POSTGRESQL_PASS**  
Password for user. This is usually not required for connections to a local db. See [pg_hba.conf] (http://www.postgresql.org/docs/9.4/static/auth-pg-hba-conf.html "host based auth") on your system to enable/disable password-less logins, if the defaults aren't working for you.

### Example

Here's a server example, which will listen for incoming HTTP requests on port `12345`, store the requests in `postgres.request_dump` (database.schema) in PostgreSQL. Replace `LOGGLY_TOKEN` with the relevant API key.

```
$ export LOGGLY_TOKEN=ffffffff-ffff-ffff-ffff-ffffffffffff
$ go build output/loggly/loggly.go
$ POSTGRESQL_DB=postgres ./loggly -port 12345
```

And here's an example of `POST`ing to that server using `cURL`, sending the contents of the file `test.json`:

```
$ echo '{"abc":123,"def":456}' > ./test.json
$ curl -XPOST -H 'Content-Type: application/json' --data @test.json http://127.0.0.1:12345/
```

Once you've `POST`ed some data, wait a couple seconds (max 10, by default), and you'll see something like this for a successful batch upload:

```
2015/10/27 11:23:40 loggly.go:62: Sent 854 bytes with status 200 OK
```

If there's an error, you'll see a dump of your request and Loggly's response. It will look something like this error, which is what shows up when a Loggly API key isn't set, and the local error checking is disabled:

```
2015/10/27 11:17:32 loggly.go:57: POST /bulk/tag/bulk/ HTTP/1.1
Host: logs-01.loggly.com
User-Agent: Go-http-client/1.1
Content-Length: 854
Accept-Encoding: gzip

POST / HTTP/1.1\nHost: 127.0.0.1:12345\nAccept: */*\nContent-Type: application/json\nUser-Agent: curl/7.43.0\n\n{ "ip_address": "12.34.56.78", "template_version": "2", "recv_method": "rest", "transmission_id": "30110032678944751", "rcpt_to": "circ-test-template@nowhere.sinkhole.msyscloud.com", "num_retries": "0", "delv_method": "esmtp", "campaign_id": "msys-test:api_transmissions_post_template", "rcpt_meta": { "binding": "test" }, "timestamp": "2015-10-22T22:25:04.000-07:00", "friendly_from": "circonus-probe@test.sinkhole.msyscloud.com", "message_id": "0001308903569a26297f", "template_id": "circonus-probe", "msg_size": "2452", "binding_group": "test", "queue_time": "162", "routing_domain": "nowhere.sinkhole.msyscloud.com", "msg_from": "bounces-template@test.sinkhole.msyscloud.com", "type": "delivery", "binding": "test21109", "rcpt_tags": []},

HTTP/1.1 403 Forbidden
Content-Length: 15
Connection: keep-alive
Content-Type: text/html
Date: Tue, 27 Oct 2015 17:17:32 GMT
Server: nginx/1.1.19

Invalid API key
```
