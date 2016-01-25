# E-Commerce Recommendation Template

## Documentation

Please refer to http://docs.prediction.io/templates/javaecommercerecommendation/quickstart/

## Versions

### v0.1.2
  add "org.jblas" dependency in build.sbt

### v0.1.1
- parallelize filtering valid items

### v0.1.0

- initial version


## Development Notes

### import sample data

```
$ python data/import_eventserver.py --access_key <your_access_key>
```

### query

normal:

```
$ curl -H "Content-Type: application/json" \
-d '{
  "userEntityId" : "u1",
  "number" : 10 }' \
http://localhost:8000/queries.json
```

```
$ curl -H "Content-Type: application/json" \
-d '{
  "userEntityId" : "u1",
  "number": 10,
  "categories" : ["c4", "c3"]
}' \
http://localhost:8000/queries.json
```

```
curl -H "Content-Type: application/json" \
-d '{
  "userEntityId" : "u1",
  "number": 10,
  "whitelist": ["i21", "i26", "i40"]
}' \
http://localhost:8000/queries.json
```

```
curl -H "Content-Type: application/json" \
-d '{
  "userEntityId" : "u1",
  "number": 10,
  "blacklist": ["i21", "i26", "i40"]
}' \
http://localhost:8000/queries.json
```

unknown user:

```
curl -H "Content-Type: application/json" \
-d '{
  "userEntityId" : "unk1",
  "number": 10}' \
http://localhost:8000/queries.json
```

### handle new user

new user:

```
curl -H "Content-Type: application/json" \
-d '{
  "userEntityId" : "x1",
  "number": 10}' \
http://localhost:8000/queries.json
```

import some view events and try to get recommendation for x1 again.

```
accessKey=<YOUR_ACCESS_KEY>
```

```
curl -i -X POST http://localhost:7070/events.json?accessKey=$accessKey \
-H "Content-Type: application/json" \
-d '{
  "event" : "view",
  "entityType" : "user"
  "entityId" : "x1",
  "targetEntityType" : "item",
  "targetEntityId" : "i2",
  "eventTime" : "2015-02-17T02:11:21.934Z"
}'

curl -i -X POST http://localhost:7070/events.json?accessKey=$accessKey \
-H "Content-Type: application/json" \
-d '{
  "event" : "view",
  "entityType" : "user"
  "entityId" : "x1",
  "targetEntityType" : "item",
  "targetEntityId" : "i3",
  "eventTime" : "2015-02-17T02:12:21.934Z"
}'

```

## handle unavailable items

Set the following items as unavailable (need to specify complete list each time when this list is changed):

```
curl -i -X POST http://localhost:7070/events.json?accessKey=$accessKey \
-H "Content-Type: application/json" \
-d '{
  "event" : "$set",
  "entityType" : "constraint"
  "entityId" : "unavailableItems",
  "properties" : {
    "items": ["i43", "i20", "i37", "i3", "i4", "i5"],
  }
  "eventTime" : "2015-02-17T02:11:21.934Z"
}'
```

Set empty list when no more items unavailable:

```
curl -i -X POST http://localhost:7070/events.json?accessKey=$accessKey \
-H "Content-Type: application/json" \
-d '{
  "event" : "$set",
  "entityType" : "constraint"
  "entityId" : "unavailableItems",
  "properties" : {
    "items": [],
  }
  "eventTime" : "2015-02-18T02:11:21.934Z"
}'
```
