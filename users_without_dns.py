#! /usr/bin/env python3

"""Find the user ids with no registered DNS traffic
"""

import os

if __name__ == "__main__":
    # Find all users with any "typical" traffic patterns
    all_users = sorted(os.listdir("data/clean/flows/typical_fqdn_DIV_user_INDEX_start"))
    dns_users = sorted(os.listdir("data/clean/dns/successful_DIV_user_INDEX_timestamp"))

    all_users = set(all_users)
    dns_users = set(dns_users)

    users_without_dns = all_users - dns_users

    print(users_without_dns)
