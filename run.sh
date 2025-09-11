#!/usr/bin/expect -f

set token "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"
set repo "https://github.com/italotec/primos.git"

spawn git clone $repo
expect "Password*:" { send "$token\r" }
interact
