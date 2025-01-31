// Copyright 2014-2022 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

// FieldType represents the type of the field in Aerospike Wire Protocol
type FieldType int

// FieldType constants used in the Aerospike Wire Protocol.
const (
	NAMESPACE            FieldType = 0
	TABLE                FieldType = 1
	KEY                  FieldType = 2
	RECORD_VERSION       FieldType = 3
	DIGEST_RIPE          FieldType = 4
	MRT_ID               FieldType = 5
	MRT_DEADLINE         FieldType = 6
	QUERY_ID             FieldType = 7
	SOCKET_TIMEOUT       FieldType = 9
	RECORDS_PER_SECOND   FieldType = 10
	PID_ARRAY            FieldType = 11
	DIGEST_ARRAY         FieldType = 12
	MAX_RECORDS          FieldType = 13
	BVAL_ARRAY           FieldType = 15
	INDEX_NAME           FieldType = 21
	INDEX_RANGE          FieldType = 22
	INDEX_CONTEXT        FieldType = 23
	INDEX_TYPE           FieldType = 26
	UDF_PACKAGE_NAME     FieldType = 30
	UDF_FUNCTION         FieldType = 31
	UDF_ARGLIST          FieldType = 32
	UDF_OP               FieldType = 33
	QUERY_BINLIST        FieldType = 40
	BATCH_INDEX          FieldType = 41
	BATCH_INDEX_WITH_SET FieldType = 42
	FILTER_EXP           FieldType = 43
)
