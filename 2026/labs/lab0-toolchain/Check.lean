-- SPDX-License-Identifier: Apache-2.0
-- Copyright (c) 2026 VakeWorks AB
--
-- Lab 0: the smallest possible machine-checked theorem. If this file
-- elaborates, your proof-checker container works. Change the 2 to a 3 and
-- run again to watch the kernel REJECT a false claim -- that rejection is
-- the whole point of the course's certificate story.

theorem one_plus_one : 1 + 1 = 2 := rfl

#eval "lab0: the kernel accepted one_plus_one -- toolchain OK"
