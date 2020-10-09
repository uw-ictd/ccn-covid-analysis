"""Tools and constants for categorizing fully qualified domain names (fqdns)
"""

import re

GOOGLE_REGEXES = {
    r'^www\.google\..*$': "Non-video Content",  # co.id, com.au, .com, .no, .cn etc.
    r'^google\..*$': "Non-video Content",
    r'^apis.google.com$': "API",
    r'^.*app-measurement\.com$': "API",  # Firebase Stats
    r'^suggestqueries\.google\.com$': "API",  # Search completion
    r'^www\.googleapis\.com$': "API",
    r'^maps\.googleapis\.com$': "API",  # Location API
    r'^mobilemaps-pa\.googleapis\.com$': "API",  # Location API
    r'^geomobileservices.*\.googleapis\.com$': "API",  # Location API
    r'^semanticlocation.*\.googleapis\.com$': "API",  # Location API
    r'^cloudconfig\.googleapis\.com$': "API",
    r'^footprints-pa\.googleapis\.com$': "API",
    r'^clientservices\.googleapis\.com$': "API",
    r'^voledevice-pa\.googleapis\.com$': "API",
    r'^.*translate.*\.com$': "API",  # Some at google apis, some at google.com
    r'^chromefeedcontentsuggestions-pa.googleapis.com$': "API",
    r'^.*crashlytics\.com$': "API",  # Firebase
    r'^phonedeviceverification-pa\.googleapis\.com$': "API",
    r'^safebrowsing\.googleapis\.com$': "API",
    r'^mdh-pa\.googleapis\.com$': "API",
    r'^mobilenetworkscoring-pa\.googleapis\.com$': "API",
    r'^firebaseremoteconfig\.googleapis\.com$': "API",
    r'^android-safebrowsing\.google\.com$': "API",
    r'^fonts\.googleapis\.com$': "API",
    r'^dns\.google.*$': "API",
    r'^people-pa\.googleapis\.com$': "API",
    r'^id\.google\..*$': "Authentication",  # Not an indonesian specific site, api front end
    r'^accounts\.google\..*$': "Authentication",
    r'^clients.*\.google\..*$': "Authentication",
    r'^cryptauthenrollment\.googleapis\.com$': "Authentication",
    r'^(?:(?!ytimg).)*googleusercontent\.com$': "Non-video Content",
    r'^(?:(?!connectivitycheck).)*gstatic\.com': "Non-video Content",  # Static
    r'^(?:(?!yt).)*ggpht\.com$': "Non-video Content",  # HTTPS everywhere lists it as related to google code and google user content.
    r'^(?:(?!beacons).)*\.gvt[0-9]*\.com$': "Mixed CDN",  # Video transcoding? and/or Chrome?
    r'^beacons.*gvt[0-9]*\.com$': "API",  # Bluetooth beacon tracking
    # r'^fonts\.gstatic\.com': "Static",
    r'^connectivitycheck\.gstatic\.com$': "API",  # Implemented as a static site but basically a check api
    r'^android\.clients\.google\.com$': "Software or Updates",
    r'^play\.googleapis\.com$': "Software or Updates",
    r'^android\.googleapis\.com$': "Software or Updates",
    r'^dl\.google\.com$': "Software or Updates",
    r'^playatoms-pa\.googleapis\.com$': "Software or Updates",  # Related to google play store and play api
    r'^update\.googleapis\.com$': "Software or Updates",
    r'^play\.google\.com$': "Software or Updates",
    r'^.*googlevideo\.com$': "Video",
    r'^.*ytimg.*$': "Video",
    r'^.*youtube.*$': "Video",
    r'^yt.*\.ggpht\.com$': "Video",  # Youtube image proxy
    r'^video\.google\..*$': "Video",
    r'^.*\.doubleclick\.net$': "Ad Network",
    r'^.*googlesyndication\.com$': "Ad Network",
    r'.*2mdn\.net': "Ad Network",  # Doubleclick
    r'^.*googletagservices\.com': "Ad Network",  # Tracking Pixels
    r'^.*googletagmanager\.com$': "Ad Network",
    r'.*analytics.*': "Ad Network",
    r'.*adservice.*': "Ad Network",  # Seen with a variety of extensions and country codes
    r'^datasaver\.googleapis\.com$': "Compressed Web",
    r'^.*ampproject\.org$': "Compressed Web",
    r'^.*ampproject\.net$': "Compressed Web",
    r'^googleweblight\.com$': "Compressed Web",
    r'^litepages\.googlezip\.net$': "Compressed Web",
    r'^ampcid\.google\..*$': "Compressed Web",  # Seems to be country specific amp helpers?
    r'^backup\.googleapis\.com$': "Files",  # User device backup...
    r'^proxy\.googlezip\.net$': "Files",
    r'^drive\.google\.com$': "Files",
    r'^photos\.googleapis\.com$': "Content Upload",
    r'^compress\.googlezip\.net$': "Content Upload",
    r'^photosdata-pa\.googleapis\.com$': "Content Upload",  # Most traffic is upstream here
    r'^.*content-storage-upload\.googleapis\.com$': "Content Upload",  # Most traffic is upstream
    r'^mail\.google\.com$': "Messaging",
    r'^inbox\.google\.com$': "Messaging",
    r'^hangouts\.google\..*$': "Messaging",
    r'^.*mtalk.*\.google\.com$': "API", # Mobile notification service
    r'^instantmessaging-pa\.googleapis\.com$': "Messaging",
    r'^.*gmail\.com$': "Messaging",
}

FACEBOOK_REGEXES = {
    r'^.*www\.facebook\.com$': "Social Media",
    r'^web\.facebook\.com$': "Social Media",
    r'^facebook\.com$': "Social Media",
    r'^m\.facebook\.com$': "Social Media",
    r'^mobile\.facebook\.com$': "Social Media",
    r'^.*video.*\.facebook\.com$': "Video",
    r'^.*video.*\.fbcdn\.net$': "Video",
    r'^static.*\.fbcdn\.net$': "Non-video Content",  # Static
    r'^static.*\.facebook\.com$': "Non-video Content",  # Static
    r'^z-m-static.*\.fbcdn\.net$': "Non-video Content",  # static
    r'^.*scontent.*\.fbcdn\.net$': "Non-video Content",
    r'^lookaside.*\.facebook\.com$': "Non-video Content",
    r'^platform-lookaside.*\.fbsbx\.com$': "Non-video Content",
    r'^.*api\.facebook\.com$': "API",
    r'^.*graph\.facebook\.com$': "Social Media",  # An api, but about social media integration, so will classify as social media
    r'^portal\.fb\.com$': "API",
    r'^connect\.facebook\.net$': "Authentication",  # Partner site embeddable sdk
    r'^connect\.facebook\.com$': "Authentication",  # Partner site embeddable sdk
    r'^.*accountkit\.com$': "Authentication",
    r'^fbsbx\.com$': "Mixed CDN",
    r'^cdn.fbsbx.com$': "Mixed CDN",
    r'^edgeray.*\.facebook\.com$': "Mixed CDN",
    r'^edge-star.*\.facebook\.com$': "Mixed CDN",
    r'^external.*\.fbcdn\.net$': "Mixed CDN",
    r'^z-m-external.*\.fbcdn\.net$': "Mixed CDN",
    r'^snaptu.*\.facebook\.com$': "Compressed Web",  # Staptu acquired to bootstrap FB-Lite
    r'^edge-snaptu.*\.facebook\.com$': "Compressed Web",
    r'^.*upload.*\.facebook\.com$': "Content Upload",
    r'^.*upload.*\.fbcdn\.com$': "Content Upload",
    r'^.*upload.*\.fbcdn\.net$': "Content Upload",
    # r'^fblive-upload\.facebook\.com': "Content Upload",
    # r'^rupload.facebook.com$': "Content Upload",
    r'^lithium.*\.facebook\.com$': "Ad Network",
    r'^an\.facebook\.com$': "Ad Network",
    r"^mqtt.*\.facebook\.com$": "Messaging",
    r'^.*edge-mqtt.*\.facebook\.com$': "Messaging",
    r'^.*edge-chat\.facebook\.com$': "Messaging",
    r'^edge-stun.*\.facebook\.com$': "Messaging",
    r'^stun.*\.facebook\.com$': "Messaging",
    r'^edge-turnservice.*\.facebook\.com$': "Messaging",
    r'^whatsapp.*\.fbcdn\.net$': "Messaging",
    r'^.*whatsapp.*\.facebook\.com$': "Messaging",
    r'^instagram.*\.fbcdn\.net': "Social Media",
    r'^instagram.*\.facebook\.com': "Social Media",
}


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
            matched_category = self.google_re[match_regex.pattern]
            if matched_category == "Authentication":
                return "API"

            return matched_category
        else:
            # Catch other small APIs after processing the main list
            if re.match(r'.*googleapis\.com', fqdn) is not None:
                return "API"

            return "Unknown (Not Mapped)"

    def _process_facebook_category(self, fqdn):
        match_regex = None
        for regex in self.facebook_compiled_re:
            if regex.match(fqdn) is not None:
                if match_regex is not None:
                    print("Duplicate Match with FQDN[{}]".format(fqdn))
                match_regex = regex

        if match_regex is not None:
            matched_category = self.facebook_re[match_regex.pattern]
            if matched_category in ["Mixed CDN", "Non-video Content"]:
                return "Social Media"

            if matched_category == "Authentication":
                return "API"

            return matched_category
        else:
            return "Unknown (Not Mapped)"

    def process_fqdn(self, fqdn):
        """Process an input domain name, returning an org and category tuple
        """
        if fqdn is None or fqdn == "":
            return "Unknown (No DNS)", "Unknown (No DNS)"

        if 'google' in fqdn or 'gmail' in fqdn or 'ytimg' in fqdn or 'youtube' in fqdn or "gstatic.com" in fqdn or "ggpht.com" in fqdn or "app-measurement.com" in fqdn or "gvt1.com" in fqdn or "ampproject" in fqdn or "crashlytics.com" in fqdn or "2mdn.net" in fqdn or "doubleclick.net" in fqdn or "1e100.net" in fqdn or "gvt2.com" in fqdn:
            return "Google", self._process_google_category(fqdn)

        if 'fbcdn' in fqdn or 'facebook' in fqdn or 'fbsbx' in fqdn or "fb.com" in fqdn or "accountkit.com" in fqdn:
            return "Facebook", self._process_facebook_category(fqdn)

        if 'whatsapp' in fqdn:
            return "WhatsApp", "Messaging"

        if 'twimg' in fqdn or 'twitter' in fqdn:
            if "video" in fqdn:
                return "Twitter", "Video"
            else:
                return "Twitter", "Social Media"

        if 'instagram' in fqdn:
            return "Instagram", "Social Media"

        if 'wikipedia' in fqdn:
            return "Wikipedia", "Non-video Content"

        if 'amazonaws' in fqdn or 'aws.com' in fqdn:
            return "Amazon Web Services", "IAAS"
        elif 'amazon' in fqdn:
            return "Amazon", "Non-video Content"

        if "livestream818.com" in fqdn:
            return "818 Religious Conference", "Video"

        if "network.bokondini" in fqdn:
            return "Local Services", "Local Services"

        if "content.bokondini" in fqdn:
            return "Local Services", "Local Services"

        if "xvideos-cdn.com" in fqdn or "xvideos.com" in fqdn:
            return "xvideos", "Adult Video"

        if "xnxx-cdn.com" in fqdn or "xnxx.com" in fqdn:
            return "xnxx", "Adult Video"

        if "trggames.com" in fqdn:
            return "TRG Games", "Games"

        if "nearme.com.cn" in fqdn:  # Redirects to oppo mobile
            return "Oppo", "API" # Location API

        if "coloros.com" in fqdn:
            # Oppo fork of android
            return "Oppo", "Software or Updates"

        if "oppomobile.com" in fqdn:
            return "Oppo", "Software or Updates"

        if "igamecj.com" in fqdn or "file-igamecj.akamaized.net" in fqdn:  # Appears to be a PUBG pirate download
            return "IgameCJ", "Games"

        if "vivoglobal.com" in fqdn or "vivo.com" in fqdn:
            return "Vivo", "Software or Updates"

        if "vidio.com" in fqdn or "vidio-com.akamaized.net" in fqdn or "vidio.static6.com" in fqdn:
            return "Vidio", "Video"

        if "tudoo.mobi" in fqdn:
            return "Tudoo", "Video"

        if "tiktokcdn.com" in fqdn or "tiktokv.com" in fqdn or "muscdn.com" in fqdn or "tiktokcdn-com" in fqdn or "byteoversea.com" in fqdn or "musical.ly" in fqdn:  # muscdn -> musicly!
            return "TikTok", "Video"

        if "topbuzzcdn.com" in fqdn:
            return "TopBuzz", "Mixed CDN"

        if "phncdn.com" in fqdn:
            return "PHN CDN", "Mixed CDN"

        if "rqmob.com" in fqdn:
            return "RQ Mob", "Ad Network"

        if "pubgmobile.com" in fqdn:
            return "PUBG", "Games"

        if "wshareit.com" in fqdn:
            return "W Share It", "Video"

        if "samsungdm.com" in fqdn:
            # Software updates
            return "Samsung", "Software or Updates"

        if "windowsupdate.com" in fqdn:
            return "Microsoft (non-azure)", "Software or Updates"

        if "dl.delivery.mp.microsoft.com" in fqdn:
            return "Microsoft (non-azure)", "Software or Updates"

        if "officecdn.microsoft.com.edgesuite.net" in fqdn:
            return "Microsoft (non-azure)", "Software or Updates"

        if "download.microsoft.com" in fqdn:
            return "Microsoft (non-azure)", "Software or Updates"

        if "azure" in fqdn:
            return "Azure (Microsoft)", "IAAS"

        if "tokopedia.net" in fqdn:
            return "Tokopedia", "Non-video Content"

        if "9appsdownloading.com" in fqdn or "9appsinstall.com" in fqdn:
            return "UC Browser", "Software or Updates"

        if "ucweb.com" in fqdn:
            return "UC Browser", "Compressed Web"

        if "liftoff.io" in fqdn:
            return "Liftoff", "Ad Network"

        if "avatar." in fqdn and ".com" in fqdn:  # Appears to be a gambling site tracker? Middle subdomain is usually random
            return "Avatar Tracker", "Ad Network"

        if "v-mate.mobi" in fqdn:  # https://techcrunch.com/2019/05/30/alibaba-vmate-100m-india/
            return "VMate", "Video"

        if "emome-ip.hinet.net" in fqdn:
            return "HiNet", "IAAS"

        if "like.video" in fqdn or "bigo.sg" in fqdn or "likeevideo.com" in fqdn or "like-video.com" in fqdn:
            return "Likee", "Video"

        if "hlssrv.com" in fqdn:
            return "HLSPlay", "Video"

        if "opera-mini.net" in fqdn or "operacdn.com" in fqdn or "transcoder.opera.com" in fqdn or "mobile.opera.com" in fqdn:
            return "Opera Mini", "Compressed Web"

        if "im-gb.com" in fqdn:  # Looks like an indo specific basic HTML news aggregator?!?
            return "Im-Gb", "Compressed Web"

        if "img.vidmatefilm.org" in fqdn:
            return "VidMateFilm", "Video"

        if "videoxcdn.net" in fqdn:
            return "VideoX", "Adult Video"

        if "bioskopview.com" in fqdn:
            return "Bioskopview", "Video"

        if "adcolony.com" in fqdn:
            return "Adcolony", "Ad Network"

        if "www2090.o0-5.com" in fqdn:
            return "Fembed", "Video"

        if "cootek.com" in fqdn:
            return "Cootek", "Mixed CDN"

        if "unityads.unity3d.com" in fqdn:
            return "Unity Ads", "Ad Network"

        if "az-dn.gw.samsungapps.com" in fqdn:
            return "Samsung", "Software or Updates"

        if "hiido.com" in fqdn:  # Chinese file sharing app
            return "Hiido", "Files"

        if "cf.shopee.co.id" in fqdn or "shopeemobile.com" in fqdn or "shopee.co.id" in fqdn:
            return "Shopee", "Non-video Content"

        if "amplitude.com" in fqdn:
            return "Amplitude", "Ad Network"

        if "ivideosmart.com" in fqdn:
            return "iVideoSmart", "Ad Network"

        if "mangatoon.100sta.com" in fqdn or "mangatoon.mobi" in fqdn or "mangatoon.akamaized.net" in fqdn:
            return "Mangatoon", "Non-video Content"

        if "ushareit" in fqdn:  # Chinese file sharing app
            return "UshareIt", "Files"

        if "au.ff.avast.com" in fqdn:
            return "Avast", "Antivirus"

        if "symantecliveupdate.com" in fqdn:
            return "Symantic", "Antivirus"

        if "definitionupdates.microsoft.com" in fqdn:
            return "Microsoft", "Antivirus"

        if "mcafee.com" in fqdn:
            return "McAfee", "Antivirus"

        if "tenor.co" in fqdn:
            return "Tenor", "Non-video Content"

        if "giphy.com" in fqdn:
            return "Giphy", "Non-video Content"

        if "qq.com" in fqdn or "myqcloud.com" in fqdn:
            return "QQ", "Messaging"

        if "idnview.com" in fqdn:  # Many prefixes... www3, www11, www9, etc.
            return "Idn View", "Video"

        if "joox.com" in fqdn:  # Music streaming
            return "Joox", "Non-video Content"

        if "baca.co.id" in fqdn:  # News
            return "Baca", "Non-video Content"

        if "weathercn.com" in fqdn:  # Weather
            return "Weather CN", "Non-video Content"

        if "vungle.com" in fqdn:
            return "Vungle", "Ad Network"

        if "iprimus.net.au" in fqdn:
            return "iPrimus", "IAAS"

        if "cdnsyy.com" in fqdn:
            return "cdn syy", "Mixed CDN"

        if "uodoo.com" in fqdn:
            return "Uodoo", "Non-video Content"

        if "applovin.com" in fqdn:
            return "AppLovin", "Ad Network"

        if "shareitgames.com" in fqdn:
            return "ShareIt Games", "Games"

        if "adjust.com" in fqdn:
            return "AdJust", "Ad Network"

        if "waptrick.org" in fqdn:  # Mobile first news, music, content aggregator
            return "WapTrick", "Non-video Content"

        if "idtribun.com" in fqdn:  # Gambling and sports
            return "Idtribun", "Games"

        if "topwin-movie-maker.com" in fqdn:
            return "Topwin", "Video"

        if "idkasino.com" in fqdn:
            return "ID Kasino", "Games"

        if "adsmoloco.com" in fqdn:
            return "Moloco", "Ad Network"

        if "menangtoto.pw" in fqdn:  # Gambling and casino games
            return "Menangtoto", "Games"

        if "ev.rdtcdn.com" in fqdn:
            return "RdtCDN", "Mixed CDN"

        if "boy18tube.com" in fqdn:
            return "Boy 18", "Adult Video"

        if "cdncollection.com" in fqdn:  # CDN associated with gambling sites
            return "CDN Collection", "Games"

        if "mopub.com" in fqdn or "snipermob.com" in fqdn:
            return "MoPub", "Ad Network"

        if "vidmate.net" in fqdn or "vidmatefilm.org" in fqdn:
            return "Vidmate", "Video"

        if "cdn.mozilla.net" in fqdn:
            return "Mozilla", "Software or Updates"

        if "pkvn.mobi" in fqdn:  # Poker
            return "Pkvn", "Games"

        if "ssacdn.com" in fqdn:
            return "Supersonic Games", "Games"

        if "pinimg.com" in fqdn or "pinterest" in fqdn:  # Photos
            return "Pinterest", "Non-video Content"

        if "carageo.com" in fqdn:  # File sharing
            return "CaraGeo", "Files"

        if "majorgeeks.com" in fqdn:  # Free software downloads
            return "MajorGeeks", "Software or Updates"

        if "slatic.net" in fqdn:  # Seems to host pictures of physical goods?
            return "Slatic", "Non-video Content"

        if "download.mediatek.com" in fqdn:
            return "Mediatek", "Software or Updates"

        if "download.adobe.com" in fqdn:
            return "Adobe", "Software or Updates"

        if "rdtcdn.com" in fqdn:
            return "Rdt CDN", "Mixed CDN"

        if "linode.com" in fqdn:
            return "Linode", "IAAS"

        if "bukalapak.com" in fqdn:
            return "Bukalapak", "Non-video Content"

        if "adtilt.com" in fqdn:
            return "Adtilt", "Ad Network"

        if "feedify.net" in fqdn:
            return "Feedify", "Ad Network"

        if "snapchat.com" in fqdn:
            return "Snapchat", "Video"

        if "alicdn.com" in fqdn:
            return "Alibaba CDN", "Mixed CDN"

        if "ksmobile.com" in fqdn:  # Phone app conglomorate
            return "KS Mobile", "Software or Updates"

        if "unrulymedia.com" in fqdn:  # Video.unrulymedia serves video ads
            return "Unruly Media", "Ad Network"

        if "blogspot.com" in fqdn:
            return "Blogspot", "Non-video Content"

        if 'akamai' in fqdn:
            return "Akamai", "Mixed CDN"

        if 'cloudfront' in fqdn:
            return "AWS Cloudfront", "Mixed CDN"

        if 'cloudflare' in fqdn:
            return "Cloudflare", "Mixed CDN"

        if 'snssdk.com' in fqdn:
            return "SN China", "API"

        if 'xiaomi.com' in fqdn:
            return "Xiaomi", "Software or Updates"

        return "Unknown (Not Mapped)", "Unknown (Not Mapped)"
