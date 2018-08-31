/*
 *  Copyright 2018 Data Artisans GmbH
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.dataartisans.streamingledger.examples.simpletrade.generator;

final class Constants {
    static final int NUM_ACCOUNTS = 1_000_000;
    static final int NUM_BOOK_ENTRIES = 1_000_000;
    static final String ACCOUNT_ID_PREFIX = "ACCT-";
    static final String BOOK_ENTRY_ID_PREFIX = "BOOK-";
    static final long MAX_ACCOUNT_TRANSFER = 10_000;
    static final long MAX_BOOK_TRANSFER = 1_000;
    static final long MIN_BALANCE = 0;

    private Constants() {
    }
}
