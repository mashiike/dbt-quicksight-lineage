generate_manifest:
	dbt compile --profiles-dir .
	cp target/manifest.json ../manifest.json
	cp target/partial_parse.msgpack ../partial_parse.msgpack
