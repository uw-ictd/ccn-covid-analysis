"""Tools and constants for categorizing fully qualified domain names (fqdns)
"""


def assign_category(fqdn):
    """Take an fqdn string and return a more general category string
    """

    if 'google' in fqdn or 'gmail' in fqdn:
        return "google"

    if 'fbcdn' in fqdn or 'facebook' in fqdn or 'fbsbx' in fqdn:
        return "facebook"

    if 'whatsapp' in fqdn:
        return "whatsapp"

    if 'twimg' in fqdn or 'twitter' in fqdn:
        return "twitter"

    if 'instagram' in fqdn:
        return "instagram"

    if 'ytimg' in fqdn or 'youtube' in fqdn:
        return "youtube"

    if 'wikipedia' in fqdn:
        return "wikipedia"

    if 'akamai' in fqdn:
        return "akamai"

    if 'amazonaws' in fqdn or 'aws' in fqdn:
        return "amazon_web_services"

    if 'cloudfront' in fqdn:
        return "cloudfront"

    if 'cloudflare' in fqdn:
        return "cloudflare"

    # No category was assigned
    return None
