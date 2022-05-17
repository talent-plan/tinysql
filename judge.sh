#!/bin/sh

if make test-proj1 > /dev/null 2>&1; then
	echo "proj-1 passed"
else
	echo "proj-1 failed"
fi

if make test-proj2 > /dev/null 2>&1; then
	echo "proj-2 passed"
else
	echo "proj-2 failed"
fi

if make test-proj3 > /dev/null 2>&1; then
	echo "proj-3 passed"
else
	echo "proj-3 failed"
fi

if make test-proj4-1 > /dev/null 2>&1; then
	echo "proj-4-1 passed"
else
	echo "proj-4-1 failed"
fi

if make test-proj4-2 > /dev/null 2>&1; then
	echo "proj-4-2 passed"
else
	echo "proj-4-2 failed"
fi

if make test-proj5-1 > /dev/null 2>&1; then
	echo "proj-5-1 passed"
else
	echo "proj-5-1 failed"
fi

if make test-proj5-2 > /dev/null 2>&1; then
	echo "proj-5-2 passed"
else
	echo "proj-5-2 failed"
fi

if make test-proj5-3 > /dev/null 2>&1; then
	echo "proj-5-3 passed"
else
	echo "proj-5-3 failed"
fi

if make proj6 > /dev/null 2>&1; then
	echo "proj-6 passed"
else
	echo "proj-6 failed"
fi


make failpoint-disable
