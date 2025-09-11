#!/usr/bin/expect -f

set token "ghp_seuTokenAqui"
set repo "https://github.com/italotec/primos.git"

spawn git clone $repo
expect "Password*:" { send "$token\r" }
interact
