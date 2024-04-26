<details>
 <summary><code>GET</code> <code><b>/curated/all</b></code> <code>(Get a month's data)</code></summary>

##### Parameters

> | name              |  type     | data type      | description                         |
> |-------------------|-----------|----------------|-------------------------------------|
> | `month-year`            |  required | string         | The date to get the curated data for     |


##### Responses

> | http code     | content-type                      | response                                                            |
> |---------------|-----------------------------------|---------------------------------------------------------------------|
> | `200`         | `application/json`                | YAML string                                                         |
> | `400`         | `application/json`                | `{"code":"400","message":"Bad Request"}`

##### Example cURL

> ```javascript
>  curl -X GET -H "Content-Type: application/json" http://localhost:8889/curated/all?month-year=01-2022
> ```

##### Example 200 Response

> ```json
>  [
>  {
>          "unit_id": "6333",
>          "dtime": "2011-02-08 19:39:43",
>          "target": "10-on-regional-detroit.samknows.comcast.net",
>          "rtt_avg": "229911.0",
>          "rtt_min": "30137.0",
>          "rtt_max": "330918.0",
>          "rtt_std": "101610.0",
>          "successes": "19",
>          "failures": "1",
>          "operator_name": "Comcast",
>          "operator_technology": "Cable",
>          "test_type": "on-net"
>        },
>  {
>          "unit_id": "5493",
>         "dtime": "2011-02-08 19:39:51",
>          "target": "21-on-regional-seattle.samknows.comcast.net",
>          "rtt_avg": "20192.0",
>          "rtt_min": "10126.0",
>          "rtt_max": "41698.0",
>          "rtt_std": "7922.0",
>          "successes": "20",
>          "failures": "0",
>          "operator_name": "Comcast",
>          "operator_technology": "Cable",
>          "test_type": "on-net"
>        }
>  ]
> ```

</details>

<details>
 <summary><code>GET</code> <code><b>/curated/filter</b></code> <code>(Get a month's data filtered based on ISP and/or technology)</code></summary>

##### Parameters

> | name              |  type     | data type      | description                         |
> |-------------------|-----------|----------------|-------------------------------------|
> | `month-year`      |  required | string         | The date to get the curated data for       |
> | `isp`        |  required | string         | ISP name       |
> | `technology`      |  required | string         | Technology type       |


##### Responses

> | http code     | content-type                      | response                                                            |
> |---------------|-----------------------------------|---------------------------------------------------------------------|
> | `200`         | `application/json`                | YAML string                                                         |
> | `400`         | `application/json`                | `{"code":"400","message":"Incorrect date"}`

##### Example cURL

> ```javascript
>  curl -X GET -H "Content-Type: application/json" http://localhost:8889/curated/filter?month-year=01-2022&isp=Comcast&technology=Cable
> ```

##### Example 200 Response

> ```json
>  [
>  {
>          "unit_id": "6333",
>          "dtime": "2011-02-08 19:39:43",
>          "target": "10-on-regional-detroit.samknows.comcast.net",
>          "rtt_avg": "229911.0",
>          "rtt_min": "30137.0",
>          "rtt_max": "330918.0",
>          "rtt_std": "101610.0",
>          "successes": "19",
>          "failures": "1",
>          "operator_name": "Comcast",
>          "operator_technology": "Cable",
>          "test_type": "on-net"
>        },
>  {
>          "unit_id": "5493",
>         "dtime": "2011-02-08 19:39:51",
>          "target": "21-on-regional-seattle.samknows.comcast.net",
>          "rtt_avg": "20192.0",
>          "rtt_min": "10126.0",
>          "rtt_max": "41698.0",
>          "rtt_std": "7922.0",
>          "successes": "20",
>          "failures": "0",
>          "operator_name": "Comcast",
>          "operator_technology": "Cable",
>          "test_type": "on-net"
>        }
>  ]
> ```

</details>

<details>
 <summary><code>GET</code> <code><b>/curated/isp</b></code> <code>(Get all ISP's for a month's of data)</code></summary>

##### Parameters

> | name              |  type     | data type      | description                         |
> |-------------------|-----------|----------------|-------------------------------------|
> | `month-year`            |  required | string         | The date to get the curated data for      |


##### Responses

> | http code     | content-type                      | response                                                            |
> |---------------|-----------------------------------|---------------------------------------------------------------------|
> | `200`         | `application/json`                | Json response                                                       |
> | `400`         | `application/json`                | `{"code":"400","message":"Incorrect date"}`

##### Example cURL

> ```javascript
>  curl -X GET -H "Content-Type: application/json" http://localhost:8889/curated/isp?month-year=01-2022
> ```

##### Example 200 Response

> ```json
>  {
>   "isp": [
>       "Comcast",
>       "Charter",
>       "Verizon"
>   ]
>  }
> ```

</details>

<details>
 <summary><code>GET</code> <code><b>/curated/technology</b></code> <code>(Get all technology for a month's of data)</code></summary>

##### Parameters

> | name              |  type     | data type      | description                         |
> |-------------------|-----------|----------------|-------------------------------------|
> | `month-year`            |  required | string         | The date to get the curated data for      |


##### Responses

> | http code     | content-type                      | response                                                            |
> |---------------|-----------------------------------|---------------------------------------------------------------------|
> | `200`         | `application/json`                | Json response                                                       |
> | `400`         | `application/json`                | `{"code":"400","message":"Incorrect date"}`

##### Example cURL

> ```javascript
>  curl -X GET -H "Content-Type: application/json" http://localhost:8889/curated/technology?month-year=01-2022
> ```

##### Example 200 Response

> ```json
>  {
>   "technology": [
>       "Fiber",
>       "Cable",
>       "DSL"
>   ]
>  }
> ```

</details>