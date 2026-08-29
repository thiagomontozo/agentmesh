CREATE INDEX agents_runtime_idx ON agents (runtime) WHERE runtime <> '';
CREATE INDEX agents_protocol_idx ON agents (protocol) WHERE protocol <> '';
