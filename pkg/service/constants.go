/*
 * Copyright 2018 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package service

// The permission resource names and the deployed filter-type spellings both live
// in lib/access now, because the Operator Development Environment applies the
// same rule and two copies would drift.

// OperatorConfigTsConn is the key Operator Lib reads its timescale DSN from, in
// operator_lib/util/model.py. A wire contract, not an internal name.
const OperatorConfigTsConn = "ts_conn"
