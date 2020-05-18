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

    if "livestream818.com" in fqdn:
        return "religious"

    if "network.bokondini" in fqdn:
        return "local services"

    if "xvideos-cdn.com" in fqdn:
        return "adult video"

    if "xnxx-cdn.com" in fqdn:
        return "adult video"

    if "trggames.com" in fqdn:
        return "games"

    if "nearme.com.cn" in fqdn:
        # Redirects to oppo mobile
        return "phone services"

    if "igamecj.com" in fqdn:
        # Appears to be a PUBG pirate download
        return "games"

    if "vivoglobal.com" in fqdn:
        return "phone services"

    if "tudoo.mobi" in fqdn:
        return "other video"

    if "9appsdownloading.com" in fqdn:
        return "UC Browser"

    if "tiktokcdn.com" in fqdn:
        return "tiktok"

    if "topbuzzcdn.com" in fqdn:
        return "other cdn"

    if "adcs.rqmob.com" in fqdn:
        return "ad network"

    if "pubgmobile.com" in fqdn:
        return "games"

    if "coloros.com" in fqdn:
        # Oppo fork of android
        return "phone services"

    if fqdn is None:
        return "No DNS"

    # No category was assigned, but there is an FQDN
    return "Other"
