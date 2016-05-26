#restorage
An experimental RESTful storage API with various pluggable backends.

The initial implementation targets elastic search, planned are mongodb, possibly dynamodb


##How to use ElasticSearch storage API

up-restorage.exe  --port=9300 --id-map="people:uuid,organisations:uuid" elastic  --index-name="concepts"  http://localhost:9200/  
up-restorage.exe  --id-map="people:uuid,organisations:uuid" elastic  --index-name="concepts"  http://localhost:9200/  
_Caviet: the order of args is important, swapping args order will  fail to start the app_

### Single Document endpoints usage
PUT http://localhost:8765/organisations/013f7fa7-aa26-3e20-84f1-fb8e5f7383ff  
```
{
"uuid": "013f7fa7-aa26-3e20-84f1-fb8e5f7383ff",
"properName": "Barclays Plc",
"identifiers": [
{
"authority": "http://api.ft.com/system/FACTSET-EDM",
"identifierValue": "05H4C8-E"
},
{
"authority": "http://api.ft.com/system/FT-UPP",
"identifierValue": "013f7fa7-aa26-3e20-84f1-fb8e5f7383ff"
},
{
"authority": "http://api.ft.com/system/LEI",
"identifierValue": "213800LBQA1Y9L22JB70"
},
{
"authority": "http://api.ft.com/system/FT-TME",
"identifierValue": "TnN0ZWluX09OX0ZvcnR1bmVDb21wYW55X0JBUkM=-T04="
}]
}
```

GET http://localhost:8080/organisations/013f7fa7-aa26-3e20-84f1-fb8e5f7383ff  
DELETE http://localhost:8080/organisations/013f7fa7-aa26-3e20-84f1-fb8e5f7383ff  

### Bulk Document endpoints usage
PUT http://localhost:8080/people/  
```
{
	"uuid": "38355379-13e8-3d7f-8567-5a6d2b7f9066",
	"name": "Rajeev Chandrasekhar",
	"salutation": "Mr.",
	"identifiers": [{
		"authority": "http://api.ft.com/system/FACTSET-PPL",
		"identifierValue": "0751XR-E"
	}, {
		"authority": "http://api.ft.com/system/FT-TME",
		"identifierValue": "M2I1OWZlYmEtNGRlNC00YjJmLTg2MTYtZDk5NDM2OGVjZWM4-UE4="
	}],
	"aliases": [
		"Rajeev Chandrasekhar"
	]
}

{
	"uuid": "f18c1d86-e188-3303-a68f-cffc28d51d13",
	"name": "Rajshree Pathy",
	"birthYear": 1956,
	"salutation": "Ms.",
	"identifiers": [{
		"authority": "http://api.ft.com/system/FT-TME",
		"identifierValue": "M2IzM2NjNWUtODcyNi00MjFjLTllMmYtZTA5ODVjMDlhNzAx-UE4="
	}, {
		"authority": "http://api.ft.com/system/FACTSET-PPL",
		"identifierValue": "06GRHC-E"
	}],
	"aliases": [
		"Rajshree Pathy"
	]
}

{
	"uuid": "12221354-1fe8-34c1-a6ce-8a7a9175f605",
	"name": "Vincent C. Gray",
	"salutation": "Mr.",
	"identifiers": [{
		"authority": "http://api.ft.com/system/FT-TME",
		"identifierValue": "M2E0MmIwM2EtMGQ0YS00NzQ4LTkwYjEtM2M2MWZiOTk0OTUw-UE4="
	}, {
		"authority": "http://api.ft.com/system/FACTSET-PPL",
		"identifierValue": "0C3ZGY-E"
	}]
}
```

GET http://localhost:8080/people/  
GET http://localhost:8080/people/__count  
DELETE http://localhost:8080/people/  
