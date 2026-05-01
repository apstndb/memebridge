//go:build memebridge_tzdata

package memebridge

import _ "time/tzdata" // embed IANA tzdata for runtimes without system zoneinfo
