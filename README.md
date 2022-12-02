# Rep2Rec
Replicated Concurrency Control and Recovery

This repository is the final project for Advanced Database Systems - Prof. Shasha.

## Installation

## Running the code

## Sample test input

```angular2html
# mini_test.txt
// Test 1.
// T2 should abort, T1 should not, because of kill youngest

begin(T1)
begin(T2)
W(T1,x1,101)
W(T2,x2,202)
W(T1,x2,102)
W(T2,x1,201)
end(T1)
dump()
```
