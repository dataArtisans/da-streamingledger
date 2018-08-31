# dA Stream Ledger

### Serializable ACID transactions on streaming data

data Artisans Streaming Ledger is a library on top of [Apache Flink](https://flink.apache.org/), for processing event streams across multiple shared states/tables with Serializable ACID Semantics.

Instead of operating on a single key in a single operator at a time (like in vanilla Apache Flink and other stream processors) data Artisans Streaming Ledger allows you to define a set of states, connect streams of events that drive the transactions, and apply flexible business logic that operates transactionally across those states.

## This repository contains the following `maven` modules:
* `da-streamingledger-sdk` - The `SDK` needed to define a streaming ledger application.
* `da-streamingledger-runtime-serial` - A simplistic serial runner, to experiment with the `SDK`.
* `da-streamingledger-examples` - Streaming ledger example programs.

A parallel runner exists as part of the dA platform,
you can learn more about the dA platform here: [dA Platform](https://data-artisans.com/platform-overview)

## Example

Let's create a simple ledger of user accounts.
An account in the ledger is identified by a `String` key,
and has a `Long` value (its balance).

We start by defining the stream ledger scope. All state definitions and transaction functions
would be bound to this named scope `"Account Ledger"`.

```java
    StreamingLedger ledger = StreamingLedger.create("Account Ledger");
```

Next, we define the accounts state.

```java
    StreamingLedger.State<String, Long> accounts = ledger.declareState("accounts")
      .withKeyType(String.class)
      .withValueType(Long.class);
```

Next, let's assume we have a `DataStream` of `TransactionEvent`s, with the following schema:

```java
final class TransactionEvent {

    private final String sourceAccountId;

    private final String targetAccountId;

    private final long accountTransfer;

    ...

    public String getSourceAccountId() {
        return sourceAccountId;
    }

    public String getTargetAccountId() {
        return targetAccountId;
    }

    public long getAccountTransfer() {
        return accountTransfer;
    }

    ...
}

```
And we would like to transfer money from the source account and the target account, in response to an incoming `TransactionEvent`.

```java
    DataStream<TransactionEvent> transactions = ...

    ledger.usingStream(transactions, "transaction stream")
    	.apply(new TxnHandler())
        .on(accounts, TransactionEvent::getSourceAccountId, "source-account", READ_WRITE)
        .on(accounts, TransactionEvent::getTargetAccountId, "target-account", READ_WRITE);
```

Where `TxnHandler` is a `TransactionProcessFunction` defined as:

```java
class TxnHandler extends TransactionProcessFunction<TransactionEvent, Void> {

        @ProcessTransaction
        public void process(
                TransactionEvent txn,
                Context<Void> transactionContext,
                @State("source-account") StateAccess<Long> sourceAccount,
                @State("target-account") StateAccess<Long> targetAccount) {

            final long sourceAccountBalance = sourceAccount.read();
            final long targetAccountBalance = targetAccount.read();

            // check the preconditions
            if (sourceAccountBalance > txn.getAccountTransfer()) {

                // compute the new balances
                long newSourceBalance = sourceAccountBalance - txn.getAccountTransfer();
                long newTargetBalance = targetAccountBalance + txn.getAccountTransfer();

                // write back the updated values
                sourceAccount.write(newSourceBalance);
                targetAccount.write(newTargetBalance);
            }
        }
    }
```

Note that `TxnHandler` will be executed with the following guaranties:
* *Atomicity:* The transaction applies all changes in an atomic manner. Either all of the modifications that the transaction function performs on the rows happen, or none.

* *Consistency:* The transaction brings the tables from one consistent state into another consistent state.

* *Isolation:* Each transaction executes as if it were the only transaction operating on the tables. Databases know different isolation levels with different guarantees. data Artisans Streaming Ledger here offers the best class: serializability.

* *Durability:* The changes made by a transaction are durable and are not lost. Durability is ensured in the same way as in other Flink applications – through persistent sources and checkpoints. In the asynchronous nature of stream processing, durability of a result can only be assumed after a checkpoint.


A more complete example can be found [here](https://github.com/dataArtisans/da-streamingledger/blob/master/da-streamingledger-examples/src/main/java/com/dataartisans/streamingledger/examples/simpletrade/SimpleTradeExample.java)

## Building from source

prerequisites:

* git
* Maven
* At least Java 8

```
git clone https://github.com/dataArtisans/da-streamingledger.git
cd da-streamingledger
mvn clean install
```

dA Stream Ledger is now available at your local `.m2` repository.

## Obtaining from Maven Central

Just add the following dependency to start experimenting with the `SDK`

```
<dependency>
  <groupId>com.data-artisans.streamingledger</groupId>
  <artifactId>da-streamingledger-sdk</artifactId>
  <version>1.0.0</version>
</dependency>
<dependency>
  <groupId>com.data-artisans.streamingledger</groupId>
  <artifactId>da-streamingledger-runtime-serial</artifactId>
  <version>1.0.0</version>
</dependency>
```

## License 

The code in this repository is under the Apache license, see [license](https://github.com/dataArtisans/da-streamingledger/blob/master/LICENSE)

