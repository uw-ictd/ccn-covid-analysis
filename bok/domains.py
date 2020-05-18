"""Tools and constants for categorizing fully qualified domain names (fqdns)
"""


def assign_category(fqdn):
    """Take an fqdn string and return a more general category string
    """

    # if 'google' in fqdn or 'gmail' in fqdn:
    #     return "google"
    #
    # if 'fbcdn' in fqdn or 'facebook' in fqdn or 'fbsbx' in fqdn:
    #     return "Facebook"

    if 'whatsapp' in fqdn:
        return "WhatsApp"

    if 'twimg' in fqdn or 'twitter' in fqdn:
        return "twitter"

    if 'instagram' in fqdn:
        return "Instagram"

    if 'ytimg' in fqdn or 'youtube' in fqdn:
        return "Video"

    if 'wikipedia' in fqdn:
        return "Wikipedia"

    if 'akamai' in fqdn:
        return "CDN"

    if 'amazonaws' in fqdn or 'aws.com' in fqdn:
        return "amazon_web_services"

    if 'cloudfront' in fqdn:
        return "CDN"

    if 'cloudflare' in fqdn:
        return "CDN"

    if "livestream818.com" in fqdn:
        return "Religious"

    if "network.bokondini" in fqdn:
        return "local services"

    if "content.bokondini" in fqdn:
        return "local services"

    if "xvideos-cdn.com" in fqdn:
        return "Adult Video"

    if "xnxx-cdn.com" in fqdn:
        return "Adult Video"

    if "xvideos.com" in fqdn:
        return "Adult Video"

    if "trggames.com" in fqdn:
        return "Games"

    if "nearme.com.cn" in fqdn:
        # Redirects to oppo mobile
        return "Device Services"

    if "igamecj.com" in fqdn:
        # Appears to be a PUBG pirate download
        return "Games"

    if "vivoglobal.com" in fqdn:
        return "Device Services"

    if "tudoo.mobi" in fqdn:
        return "Video"

    if "9appsdownloading.com" in fqdn:
        return "UC Browser"

    if "tiktokcdn.com" in fqdn:
        return "Video"

    if "tiktokv.com" in fqdn:
        return "Video"

    if "topbuzzcdn.com" in fqdn:
        return "CDN"

    if "phncdn.com" in fqdn:
        return "CDN"

    if "adcs.rqmob.com" in fqdn:
        return "Ad Network"

    if "pubgmobile.com" in fqdn:
        return "Games"

    if "coloros.com" in fqdn:
        # Oppo fork of android
        return "Device Services"

    if "connectivitycheck.gstatic.com" in fqdn:
        return "Device Services"

    if "fonts.gstatic.com" in fqdn:
        return "Fonts"

    if "ggpht.com" in fqdn:
        # HTTPS everywhere lists it as related to google code and google user content.
        return "Device Services"

    if "wshareit.com" in fqdn:
        return "Video"

    if "oppomobile.com" in fqdn:
        return "Device Services"

    if "samsungdm.com" in fqdn:
        # Software updates
        return "Device Services"

    if "windowsupdate.com" in fqdn:
        return "Device Services"

    if "app-measurement.com" in fqdn:
        # Google Firebase
        return "Device Services"

    if "gvt1.com" in fqdn:
        # Google Chrome updates
        return "Device Services"

    if "tokopedia.net" in fqdn:
        return "Shopping"

    if "ucweb.com" in fqdn:
        return "UC Browser"

    if "gstatic.com" in fqdn:
        return "google"

    if "liftoff.io" in fqdn:
        return "Ad Network"

    # No category was assigned, but there is an FQDN
    return "Other"
