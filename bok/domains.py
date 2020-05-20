"""Tools and constants for categorizing fully qualified domain names (fqdns)
"""

import re

GOOGLE_REGEXES = {
    r'^www\.google\.com$': "Main Site",
    r'^android\.clients\.google\.com$': "Android",
    r'^www\.googleapis\.com$': "API",
    r'^play\.googleapis\.com$': "Play API",
    r'^.*googleusercontent\.com$': "Cache",
    r'^.*googlevideo\.com$': "Video",
    r'^android\.googleapis\.com$': "Android API",
    r'^dl\.google\.com$': "Software Update",
    r'^googleads.*\.doubleclick\.net$': "Ad Network",
    r'^mtalk\.google\.com$': "Messaging",
    r'^geomobileservices.*\.googleapis\.com$': "Location API",
    r'^semanticlocation.*\.googleapis\.com$': "Location API",
    r'^cloudconfig\.googleapis\.com$': "API",
    r'^playatoms-pa\.googleapis\.com$': "API",
    r'^accounts\.google\.com$': "Authentication",
    r'^people-pa\.googleapis\.com$': "API",
    r'^instantmessaging-pa\.googleapis\.com$': "Messaging",
    r'^.*ytimg.*$': "Video",
    r'^.*youtube.*$': "Video",
    r'^clients.*\.google\.com$': "Authentication",
    r'^gstatic\.com': "Static",
    r'^connectivitycheck\.gstatic\.com$': "Device Services",
    r'^fonts\.gstatic\.com': "Static",
    r'^ggpht\.com$': "SDK",  # HTTPS everywhere lists it as related to google code and google user content.
    r'^app-measurement\.com$': "API",  # Firebase Stats
    r'gvt1\.com': "Device Services"  # Chrome Updates
}

FACEBOOK_REGEXES = {
    r'^video.*\.fbcdn\.net$': "Video",
    r'^static.*\.fbcdn\.net$': "Static",
    r'^scontent.*\.fbcdn\.net$': "Photos",
    r'^lookaside.*\.facebook\.com$': "Photos",
    r'^platform-lookaside.*\.fbsbx\.com$': "Photos",
    r'^external.*\.fbcdn\.net$': "Mixed CDN",
    r'^lithium.*\.facebook\.com$': "Ad Network",
    r'^.*api\.facebook\.com$': "API",
    r'^.*graph\.facebook\.com$': "API",
    r'^edgeray.*\.facebook\.com$': "Mixed CDN",
    r'^connect\.facebook\.net$': "Embedded SDK",  # Partner site embeddable sdk
    r'^edge-mqtt\.facebook\.com$': "Messaging",
    r'^snaptu-mini\.facebook\.com$': "Lite SDK",  # Acquired for FB-Lite
    r'^rupload.facebook.com$': "Content Upload",
    r'^edge-turnservice.*\.facebook\.com$': "Mixed CDN",
    r'^www\.facebook\.com$': "Main Site",
    r'^web\.facebook\.com $': "Main Site",
    r'^an\.facebook\.com$': "Ad Network",
    r'^cdn.fbsbx.com$': "Mixed CDN",
    r'^m\.facebook\.com$': "Main Site",
}



"2.tlu.dl.delivery.mp.microsoft.com",

"au.ff.avast.com",

class FqdnProcessor(object):
    """An object class holding compiled matcher state
    """
    def __init__(self):
        self.google_re = GOOGLE_REGEXES
        self.google_compiled_re = [re.compile(x) for x in self.google_re.keys()]
        self.facebook_re = FACEBOOK_REGEXES
        self.facebook_compiled_re = [re.compile(x) for x in self.facebook_re.keys()]

    def _process_google_category(self, fqdn):
        match_regex = None
        for regex in self.google_compiled_re:
            if regex.match(fqdn) is not None:
                if match_regex is not None:
                    print("Duplicate Match with FQDN[{}]".format(fqdn))
                match_regex = regex

        if match_regex is not None:
            return self.google_re[match_regex.pattern]
        else:
            return "Other"

    def _process_facebook_category(self, fqdn):
        match_regex = None
        for regex in self.facebook_compiled_re:
            if regex.match(fqdn) is not None:
                if match_regex is not None:
                    print("Duplicate Match with FQDN[{}]".format(fqdn))
                match_regex = regex

        if match_regex is not None:
            return self.facebook_re[match_regex.pattern]
        else:
            return "Other"

    def process_fqdn(self, fqdn):
        """Process an input domain name, returning an org and category tuple
        """
        if 'google' in fqdn or 'gmail' in fqdn or 'ytimg' in fqdn or 'youtube' in fqdn or "gstatic.com" in fqdn or "ggpht.com" in fqdn or "app-measurement.com" in fqdn or "gvt1.com" in fqdn:
            return "Google", self._process_google_category(fqdn)
        elif 'fbcdn' in fqdn or 'facebook' in fqdn or 'fbsbx' in fqdn:
            return "Facebook", self._process_facebook_category(fqdn)

        if 'whatsapp' in fqdn:
            return "WhatsApp", "Messaging"

        if 'twimg' in fqdn or 'twitter' in fqdn:
            return "twitter", "Messaging"

        if 'instagram' in fqdn:
            return "Instagram", "Mixed"

        if 'wikipedia' in fqdn:
            return "Wikipedia", "Mixed"

        if 'akamai' in fqdn:
            return "Akamai", "Mixed CDN"

        if 'amazonaws' in fqdn or 'aws.com' in fqdn:
            return "amazon_web_services", "Other"

        if 'cloudfront' in fqdn:
            return "Cloudfront", "Mixed CDN"

        if 'cloudflare' in fqdn:
            return "Cloudflare", "Mixed CDN"

        if "livestream818.com" in fqdn:
            return "Religious", "Video"

        if "network.bokondini" in fqdn:
            return "Local Services", "Shopping"

        if "content.bokondini" in fqdn:
            return "Local Services", "Video"

        if "xvideos-cdn.com" in fqdn or "xvideos.com" in fqdn:
            return "xvideos", "Adult Video"

        if "xnxx-cdn.com" in fqdn:
            return "xnxx", "Adult Video"

        if "trggames.com" in fqdn:
            return "TRG Games", "Games"

        if "nearme.com.cn" in fqdn:  # Redirects to oppo mobile
            return "Oppo", "Location API"

        if "coloros.com" in fqdn:
            # Oppo fork of android
            return "Oppo", "Device Services"

        if "oppomobile.com" in fqdn:
            return "Oppo", "Device Services"

        if "igamecj.com" in fqdn:  # Appears to be a PUBG pirate download
            return "Other", "Games"

        if "vivoglobal.com" in fqdn:
            return "Vivo", "Device Services"

        if "tudoo.mobi" in fqdn:
            return "Tudoo", "Video"

        if "tiktokcdn.com" in fqdn or "tiktokv.com" in fqdn:
            return "TikTok", "Video"

        if "topbuzzcdn.com" in fqdn:
            return "TopBuzz", "Mixed CDN"

        if "phncdn.com" in fqdn:
            return "PHN CDN", "Mixed CDN"

        if "adcs.rqmob.com" in fqdn:
            return "RQ Mob", "Ad Network"

        if "pubgmobile.com" in fqdn:
            return "PUBG", "Games"

        if "wshareit.com" in fqdn:
            return "W Share It", "Video"

        if "samsungdm.com" in fqdn:
            # Software updates
            return "Samsung", "Device Services"

        if "windowsupdate.com" in fqdn:
            return "Microsoft", "Device Services"

        if "tokopedia.net" in fqdn:
            return "Tokopedia", "Shopping"

        if "9appsdownloading.com" in fqdn or "ucweb.com" in fqdn:
            return "UC Browser", "Other"

        if "liftoff.io" in fqdn:
            return "Liftoff", "Ad Network"

        if "avatar.96nmdqufhz.com" in fqdn:  # Appears to be a gambling site tracker?
            return "Other", "Games"

        if "v-mate.mobi" in fqdn:  # https://techcrunch.com/2019/05/30/alibaba-vmate-100m-india/
            return "VMate", "Video"

        return "Other", "Other"

