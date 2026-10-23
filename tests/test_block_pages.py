"""Block pages served as "success" (Mimic PR #241, 2026-10-23): the SDK's classifier, its
``is_blocked`` and its ``challenge_family`` agree with Mimic's.

``is_blocked`` is the vendored classifier's verdict (routing/diagnosis.py, VERSION 4), so Mimic's new
``is_blocked`` rules reach the SDK with the re-copy: the challenge titles, the vendors' challenge
headers, Imperva's "Pardon Our Interruption", Vercel's checkpoint on 429, "verifying your browser"
proof-of-work pages, the small-page geo wall and text measured without scripts. ``challenge_family``
follows Mimic's ``challenge.classify_challenge`` / ``api_family_of``.

Every page here is synthetic (Mimic's tests/test_block_pages.py): the shape (title, markup, markers,
size) of what the site served, with made-up ids and no token, cookie or captured value.
"""
import re

import pytest

from scrapy_calyprium.routing import diagnosis as dg
from scrapy_calyprium.routing.block_detect import challenge_family, diagnose, is_blocked

HTML = "text/html; charset=utf-8"
FILLER = "<p>Lorem ipsum dolor sit amet, product description text.</p>" * 400
NAV = '<nav><a href="/">Home</a><a href="/shop">Shop</a><a href="/cart">Cart</a></nav>'
CSS = "<style>" + ".a{margin:0;padding:0;font:16px Arial,sans-serif;color:#333}" * 300 + "</style>"


def page(title: str, head: str = "", body: str = "") -> str:
    return (f"<!DOCTYPE html><html><head><title>{title}</title>{head}</head>"
            f"<body>{NAV}{body}{FILLER}</body></html>")


DATADOME_CAPTCHA = (
    '<html lang="en"><head><title>example.com</title><style>#cmsg{animation: A 1.5s;}</style></head>'
    '<body style="margin:0"><p id="cmsg">Please enable JS and disable any ad blocker</p>'
    "<script data-cfasync=\"false\">var dd={'rt':'c','cid':'AHrlqAAAexample','hsh':'0123ABCD','t':'fe',"
    "'qp':'','s':12345,'e':'example','host':'geo.captcha-delivery.com'}</script>"
    '<script data-cfasync="false" src="https://ct.captcha-delivery.com/c.js"></script></body></html>')
DATADOME_BANNED = DATADOME_CAPTCHA.replace("'t':'fe'", "'t':'bv'")

AKAMAI_BM_VERIFY = (
    '<!DOCTYPE html><html><head> <meta charset="utf-8"> <meta http-equiv="refresh" '
    "content=\"5; URL='/us/?bm-verify=EXAMPLE'\" /><title>&nbsp;</title><script> var i = 1700000000; "
    'var j = i + Number("1234" + "56789"); </script> </head> <noscript> <iframe style="border: none;" '
    'src=""></iframe></noscript><body> <iframe style="border: none; width: 100vw; height: 100vh;" '
    'src="/interstitial/ic.html"> </iframe> <script> function verify() {var xhr = new XMLHttpRequest(); '
    'xhr.withCredentials = true; xhr.addEventListener("loadend", function() { window.location.reload(); }); '
    'xhr.open("POST", "/_sec/verify?provider=interstitial", false); '
    'xhr.setRequestHeader("Content-Type", "application/json"); '
    'xhr.send(JSON.stringify({"bm-verify": "EXAMPLE", "pow": j}));} verify();</script>  </body> </html>')
AKAMAI_SENSOR_SHELL = (
    '<!DOCTYPE html><html><head><meta charset="utf-8"><title></title></head><body>'
    '<script type="text/javascript" src="/aB3d/Ef9H/Jk2L/Mn4P/Qr6S/Tu8V"></script>'
    '<noscript><img src="/akam/13/pixel_1a2b3c4d?a=ZXhhbXBsZQ==" style="visibility: hidden" /></noscript>'
    "</body></html>")
AKAMAI_PIXEL_SHELL = (
    '<html><head></head><body><script type="text/javascript">var bazadebezolkohpepadr="1234567890";</script>'
    '<script type="text/javascript" src="https://www.example.com/akam/13/1a2b3c4d" defer></script>'
    "</body></html>")
AKAMAI_SOMETHING_WENT_WRONG = (
    "<!DOCTYPE html><html><head><title>Something went wrong</title></head><body><header>"
    '<img src="/logo.svg" alt="Example"></header><main><h1>Something went wrong</h1>'
    "<p>Please try again later.</p><p>Reference&#32;&#35;18&#46;1a2b3c4d&#46;1700000000&#46;5e6f7a8b</p>"
    "</main><footer>Example Inc.</footer></body></html>")

# office depot answers exits outside its country with this, titled "Site Maintenance".
GEO_SITE_MAINTENANCE = (
    "<!doctype html><html><head><title>Site Maintenance</title><style>body{font:16px Arial}</style>"
    '</head><body><header><img src="https://media.example.com/logo.png" alt="Example Store" /></header>'
    "<main><h1>We are sorry, but Example Store is currently not available in your country.<br /></h1>"
    "<p>Please contact the site administrator</p></main><footer><p>Reference Code: <strong>1</strong></p>"
    "</footer></body></html>")

# Imperva's reese84 interstitial, in a <main> (heb.com) or not.
IMPERVA_PARDON_MAIN = (
    "<!DOCTYPE html><html><head><noscript><title>Pardon Our Interruption</title></noscript>"
    '<meta name="robots" content="noindex, nofollow">' + CSS[:3000] + "</style>"
    "<script>window.reeseSkipExpirationCheck = true; const isSpa = window.isImpervaSpaSupport;"
    "window.onProtectionInitialized = function(protection) { if (protection && protection.cookieIsSet "
    "&& !protection.cookieIsSet()) { showBlockPage(); return; } window.location.reload(true); };</script>"
    "<script>var s = document.createElement('script'); s.src = '/Example-Words-Joined-By-Dashes?s=abc';"
    "document.head.appendChild(s);</script></head><body><div class=\"container\">"
    '<main class="imperva-block-page"><section class="frame"><h1>We noticed something unusual</h1>'
    "<p>Something about your browsing behavior made our system think you were a bot.</p><ul>"
    "<li>You're browsing faster than typical human speed</li><li>Cookies are disabled</li></ul>"
    "</section></main></div><script>function showBlockPage() { document.title = "
    '"Pardon Our Interruption"; }</script></body></html>')
IMPERVA_PARDON = (
    "<!DOCTYPE html><html><head><title>Pardon Our Interruption</title><style>h1{color:#2a2d30}</style>"
    "<script>window.reeseSkipExpirationCheck = true; window.onProtectionInitialized = function(p) "
    "{ window.location.reload(true); };</script>"
    '<script type="text/javascript" src="/example-cdn" async defer></script></head><body>'
    '<div class="container"><h1>Pardon Our Interruption</h1><p>As you were browsing something about your '
    "browser made us think you were a bot. There are a few reasons this might happen:</p><ul>"
    "<li>You've disabled JavaScript in your web browser.</li><li>You're a power user moving through this "
    "website with super-human speed.</li><li>A third-party browser plugin is preventing JavaScript from "
    "running. Additional information is available in this <a href='https://docs.imperva.com/x'>support "
    "article</a>.</li></ul></div></body></html>")
IMPERVA_UTMVC = ('<html><head><script src="/_Incapsula_Resource?SWJIYLWA=0123456789abcdef0123456789abcdef">'
                 "</script></head><body>Request unsuccessful.</body></html>")

# Vercel's checkpoint: ~30KB of CSS and inline script, a <main> and a <footer>, served with 429.
VERCEL_CHECKPOINT = (
    '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><title>Vercel Security Checkpoint</title>'
    + CSS * 2 + '</head><body><div id="root" class="container"><main><div class="spinner"></div>'
    "<p id=\"header-text\">We're verifying your browser</p><div id=\"fix-container\" style=\"display: none;\">"
    '<a id="fix-text" href="https://vercel.com/docs/example">Website owner? Click here to fix</a></div>'
    '</main><footer><p id="footer-text">Vercel Security Checkpoint</p><p>|</p><p>iad1::example</p></footer>'
    '</div></body></html><script type="module">function e(){return 1}e();</script>')

# A site's own SHA-256 proof of work: one sentence and ~4KB of solver script, posted back as a form.
POW_INTERSTITIAL = (
    "<!DOCTYPE html><html><head><title></title></head><body><noscript><p>Please enable JavaScript and "
    "reload the page to continue.</p></noscript><p>Verifying your browser, you will be redirected "
    "shortly&hellip;</p><script>var token = \"EXAMPLE.TOKEN\"; var seed = \"EXAMPLESEED\"; var difficulty = 4;"
    + "function sha256(ascii) { var maxWord = Math.pow(2, 32); var words = []; return ascii; }\n" * 40
    + "function submit(solution) { var form = document.createElement('form'); form.method = 'POST'; "
    "form.action = ''; document.body.appendChild(form); form.submit(); }</script></body></html>")

# AWS WAF's challenge as booking.com serves it: 202, an empty title, challenge.js from a first-party path.
AWS_WAF_202 = (
    '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><title></title>' + CSS[:3500] + "</style>"
    "<script>window.awsWafCookieDomainList = ['example.com'];</script>"
    '<script src="https://www.example.com/__challenge_EXAMPLE/0a1b2c3d4e5f/6a7b8c9d0e1f/challenge.js">'
    "</script></head><body><div id=\"challenge-container\"></div><noscript><h1>JavaScript is disabled</h1>"
    "In order to continue, we need to verify that you're not a robot.</noscript><script>"
    "AwsWafIntegration.saveReferrer(); AwsWafIntegration.checkForceRefresh().then((forceRefresh) => { "
    "if (forceRefresh) { AwsWafIntegration.forceRefreshToken().then(() => { window.location.reload(true) }) "
    "} else { AwsWafIntegration.getToken().then(() => { window.location.reload(true) }) } });</script>"
    "</body></html>")
AWS_WAF_202_TITLED = AWS_WAF_202.replace("<title></title>", "<title>Example Travel</title>").replace(
    "<body>", '<body><header><nav><a href="/">Home</a><a href="/a">A</a><a href="/b">B</a></nav></header>')

# PerimeterX's block page whose container sits behind a large stylesheet (past an 8KB window).
PX_LATE = (
    '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><title>Access to this page has been denied'
    "</title>" + CSS + "</head><body><section><h1>Press &amp; Hold to confirm you are a human</h1>"
    '<div id="px-captcha"></div></section><script>window._pxAppId = "PXexample1"; '
    "window._pxJsClientSrc = '/example1/init.js';</script></body></html>")
PX_LATE_NO_CONTAINER = PX_LATE.replace('<div id="px-captcha"></div>', "").replace(
    "Press &amp; Hold to confirm you are a human", "Please verify")

# Fastly Next-Gen WAF's client challenge (la-z-boy.com, hornbach.de): ~3KB, served with 200.
FASTLY = ("<!DOCTYPE html><html lang=\"en\"><head>"
          '<link href="/_fs-ch-1T1wmsGaOgGaSxcX/assets/styles.css" rel="stylesheet" />'
          "<title>Client Challenge</title></head><body><noscript>JavaScript is disabled in your browser."
          "</noscript><script>loadScript('/_fs-ch-1T1wmsGaOgGaSxcX/errors.js').then(() => {"
          "const script = document.createElement('script');"
          "script.src = '/_fs-ch-1T1wmsGaOgGaSxcX/script.js?reload=true';"
          "document.body.appendChild(script);});</script></body></html>")

CF_CHALLENGE = (
    '<!DOCTYPE html><html lang="en-US"><head><title>Just a moment...</title></head><body>'
    "<script>(function(){window._cf_chl_opt={cvId: '3',cZone: 'www.example.com',cType: 'managed'};"
    "var cpo=document.createElement('script');"
    "cpo.src='/cdn-cgi/challenge-platform/h/g/orchestrate/chl_page/v1?ray=8a';}());</script></body></html>")
# Cloudflare's JavaScript-detections tag: it rides ordinary pages, and other vendors' block pages of a
# site behind Cloudflare.
JSD = "<script src='/cdn-cgi/challenge-platform/scripts/jsd/main.js'></script>"


# (name, status, body, headers, class, vendor, evidence or None, challenge family)
CASES = [
    ("datadome captcha 200", 200, DATADOME_CAPTCHA, None, dg.CHALLENGE, dg.DATADOME, "datadome_challenge",
     "datadome"),
    ("datadome banned 200", 200, DATADOME_BANNED, None, dg.EDGE_DENY, dg.DATADOME, "datadome_banned",
     "datadome"),
    ("akamai bm-verify 200", 200, AKAMAI_BM_VERIFY, {"set-cookie": "ak_bmsc=x"}, dg.CHALLENGE, dg.AKAMAI,
     "akamai_interstitial", None),
    ("akamai sensor-script shell 200", 200, AKAMAI_SENSOR_SHELL, None, dg.CHALLENGE, dg.AKAMAI,
     "akamai_sensor_shell", "akamai"),
    ("akamai pixel shell 200", 200, AKAMAI_PIXEL_SHELL, None, dg.CHALLENGE, dg.AKAMAI, "akamai_sensor_shell",
     "akamai"),
    ("akamai 'something went wrong' 200", 200, AKAMAI_SOMETHING_WENT_WRONG, None, dg.EDGE_DENY, dg.AKAMAI,
     "akamai_deny", "akamai"),
    ("imperva pardon in a <main> 200", 200, IMPERVA_PARDON_MAIN, {"x-iinfo": "1-2-3 NNNN"}, dg.CHALLENGE,
     dg.IMPERVA, "imperva_interstitial", None),
    ("imperva pardon 200", 200, IMPERVA_PARDON, None, dg.CHALLENGE, dg.IMPERVA, "imperva_interstitial", None),
    ("imperva legacy utmvc 403", 403, IMPERVA_UTMVC, None, dg.CHALLENGE, dg.IMPERVA, "imperva_utmvc",
     "incapsula"),
    ("vercel checkpoint 429", 429, VERCEL_CHECKPOINT, None, dg.CHALLENGE, dg.VERCEL, "vercel_checkpoint", None),
    ("vercel checkpoint 429 by header", 429, VERCEL_CHECKPOINT.replace("Vercel Security Checkpoint", "x"),
     {"x-vercel-mitigated": "challenge"}, dg.CHALLENGE, dg.VERCEL, "vercel_checkpoint", None),
    ("proof-of-work interstitial 200", 200, POW_INTERSTITIAL, None, dg.CHALLENGE, None, "js_interstitial",
     None),
    ("aws waf 202 first-party", 202, AWS_WAF_202, None, dg.CHALLENGE, dg.AWS_WAF, "aws_waf_challenge",
     "aws_waf"),
    ("aws waf 202 in a titled page", 202, AWS_WAF_202_TITLED, None, dg.CHALLENGE, dg.AWS_WAF,
     "aws_waf_challenge", "aws_waf"),
    ("perimeterx block 200, container past 8KB", 200, PX_LATE, None, dg.CHALLENGE, dg.PERIMETERX,
     "perimeterx_challenge", "perimeterx"),
    ("perimeterx block 200, title and app id only", 200, PX_LATE_NO_CONTAINER, None, dg.CHALLENGE,
     dg.PERIMETERX, "perimeterx_challenge", "perimeterx"),
    ("perimeterx block 403", 403, PX_LATE, None, dg.CHALLENGE, dg.PERIMETERX, "perimeterx_challenge",
     "perimeterx"),
    ("fastly client challenge 200", 200, FASTLY, None, dg.CHALLENGE, dg.FASTLY, "fastly_challenge", "fastly"),
]
IDS = [c[0] for c in CASES]


@pytest.mark.parametrize("name,status,body,headers,cls,vendor,evidence,family", CASES, ids=IDS)
def test_the_block_is_blocked(name, status, body, headers, cls, vendor, evidence, family):
    assert is_blocked(status, body, content_type=HTML, headers=headers) is True
    assert is_blocked(status, body.encode("utf-8"), content_type=HTML, headers=headers) is True


@pytest.mark.parametrize("name,status,body,headers,cls,vendor,evidence,family", CASES, ids=IDS)
def test_the_block_gets_its_class_and_vendor(name, status, body, headers, cls, vendor, evidence, family):
    d = diagnose(status, body, HTML, headers)
    assert d.cls == cls, d
    if vendor is not None:
        assert d.vendor == vendor, d
    if evidence is not None:
        assert d.evidence == evidence, d


@pytest.mark.parametrize("name,status,body,headers,cls,vendor,evidence,family", CASES, ids=IDS)
def test_the_block_names_the_challenge_its_solver_clears(name, status, body, headers, cls, vendor, evidence,
                                                         family):
    assert challenge_family(status, body, headers) == family
    assert challenge_family(status, body.encode("utf-8"), headers) == family


def test_the_geo_wall_is_a_wall_not_content():
    """office depot's 200 "Site Maintenance" page with its own header, <main> and footer. A wall,
    as in Mimic, where no solve helps (an exit in the right country does): the SDK's is_blocked
    leaves walls to the diagnosis (move: stop), which no longer calls the page ok."""
    d = diagnose(200, GEO_SITE_MAINTENANCE, HTML)
    assert (d.cls, d.evidence, d.info.move) == (dg.GEO_OR_CONSENT, "geo_wall", dg.STOP)
    assert is_blocked(200, GEO_SITE_MAINTENANCE, content_type=HTML) is False
    assert challenge_family(200, GEO_SITE_MAINTENANCE) is None


def test_a_vercel_429_is_not_a_rate_limit():
    assert diagnose(429, VERCEL_CHECKPOINT, HTML).cls == dg.CHALLENGE
    assert diagnose(429, "<html>Too many requests</html>", HTML).cls == dg.RATE_LIMITED


# -- Mimic's is_blocked rules, as the SDK sees them --

@pytest.mark.parametrize("title", ["Pardon Our Interruption", "Access to this page has been denied",
                                   "Vercel Security Checkpoint", "Verifying your browser", "Client Challenge",
                                   "Just a moment...", "One moment, please"])
def test_a_challenge_title_is_not_a_real_page_in_any_layout(title):
    """A challenge's title on a page with little text is blocked, though the page sits in layout tags."""
    body = (f"<html><head><title>{title}</title></head><body><header>Example</header><main>"
            "<p>Please wait while we check your connection.</p></main><footer>x</footer></body></html>")
    assert is_blocked(200, body, content_type=HTML) is True


def test_an_entity_only_title_is_no_title():
    """Akamai's interstitial is titled "&nbsp;": that is not a real page's title."""
    assert diagnose(200, AKAMAI_BM_VERIFY, HTML).cls == dg.CHALLENGE


@pytest.mark.parametrize("headers,vendor", [
    ({"cf-mitigated": "challenge"}, dg.CLOUDFLARE),
    ({"cf-mitigated": "['challenge']"}, dg.CLOUDFLARE),  # a header httpcloak hands back as a list
    ({b"cf-mitigated": [b"challenge"]}, dg.CLOUDFLARE),  # Scrapy's headers
    ({"x-vercel-mitigated": "challenge"}, dg.VERCEL),
], ids=["cf-mitigated", "cf-mitigated as a list", "cf-mitigated as scrapy headers", "x-vercel-mitigated"])
def test_a_vendors_challenge_header_blocks_a_real_page(headers, vendor):
    body = page("Widget | Example Store")
    assert is_blocked(200, body, content_type=HTML) is False
    d = diagnose(200, body, HTML, headers)
    assert (d.cls, d.vendor) == (dg.CHALLENGE, vendor)


def test_aws_wafs_challenge_header_blocks_its_answers():
    """x-amzn-waf-action names the action AWS WAF took: its captcha (405) and challenge (202)."""
    stub = "<html><head><title></title></head><body><div id='challenge-container'></div></body></html>"
    assert diagnose(405, stub, HTML, {"x-amzn-waf-action": "captcha"}).cls == dg.CHALLENGE
    assert is_blocked(202, stub, content_type=HTML, headers={"x-amzn-waf-action": "challenge"}) is True
    assert challenge_family(405, stub, {"x-amzn-waf-action": "captcha"}) == "aws_waf"
    assert challenge_family(202, stub, {"x-amzn-waf-action": "challenge"}) == "aws_waf"


def test_visible_text_is_measured_without_scripts():
    """The proof-of-work page carries ~4KB of solver script around one sentence: not content."""
    tag_stripped = " ".join(re.sub(r"<[^>]+>", " ", POW_INTERSTITIAL).split())
    assert len(tag_stripped) > 3000  # what a count that only strips tags reads as text
    assert is_blocked(200, POW_INTERSTITIAL, content_type=HTML) is True
    assert diagnose(200, POW_INTERSTITIAL, HTML).evidence == "js_interstitial"


# -- challenge_family: the order of Mimic's challenge.classify_challenge --

@pytest.mark.parametrize("status,body,family", [
    (403, PX_LATE + JSD, "perimeterx"),
    (200, DATADOME_CAPTCHA + JSD, "datadome"),
    (200, FASTLY.replace("</body>", JSD + "</body>"), "fastly"),
    (202, AWS_WAF_202 + JSD, "aws_waf"),
    (200, IMPERVA_PARDON.replace("</body>", JSD + "</body>"), None),
    (200, AKAMAI_BM_VERIFY.replace("</body>", JSD + "</body>"), None),
], ids=["perimeterx", "datadome", "fastly", "aws_waf", "imperva pardon", "akamai bm-verify"])
def test_another_vendors_page_behind_cloudflare_is_that_vendors(status, body, family):
    assert challenge_family(status, body) == family


def test_cloudflare_is_still_cloudflare():
    assert challenge_family(403, CF_CHALLENGE) == "cloudflare_managed"
    deny = "<html>" + JSD + "<p>Sorry, you have been blocked</p></html>"
    assert challenge_family(403, deny) == "cloudflare_managed"
    assert challenge_family(403, page("Shop", body=JSD), {"cf-mitigated": "['challenge']"}) == "cloudflare_managed"
    # Cloudflare's own challenge on a page that also names another vendor stays Cloudflare's.
    assert challenge_family(403, CF_CHALLENGE + "<div id='px-captcha'></div>") == "cloudflare_managed"


def test_akamais_200_pages():
    assert challenge_family(200, AKAMAI_SENSOR_SHELL) == "akamai"
    assert challenge_family(200, AKAMAI_SOMETHING_WENT_WRONG) == "akamai"
    # A real page that quotes a reference id is not Akamai's deny.
    real = ("<html><head><title>Order status | Example</title></head><body><nav><a href='/'>Home</a>"
            "<a href='/a'>A</a><a href='/b'>B</a></nav>" + "<p>Your order details and history.</p>" * 120
            + "<p>Reference-ID: #0.5f891402.1791225891.7f48394a</p></body></html>")
    assert challenge_family(200, real) is None
    # The stock deny page escapes its reference id; on 403 it names Akamai as before.
    assert challenge_family(403, "<html><body>Reference&#32;&#35;18&#46;1a2b3c4d&#46;1700000000</body></html>") \
        == "akamai"


def test_aws_waf_only_on_its_own_statuses():
    assert challenge_family(403, AWS_WAF_202) is None
    assert challenge_family(405, AWS_WAF_202) == "aws_waf"


def test_fastly_needs_its_loader():
    assert challenge_family(200, FASTLY + "x" * 60_000) is None  # a large page naming the path
    assert challenge_family(200, FASTLY.replace("script.js", "app.js")) is None


# Pages these rules must leave alone.
NOT_BLOCKED = [
    ("a real page with the akamai pixel tag", 200,
     page("Cleaning Products - Example",
          head='<script type="text/javascript">var bazadebezolkohpepadr="1234567890";</script>'
               '<script type="text/javascript" src="https://www.example.com/akam/13/1a2b3c4d" defer></script>')),
    ("a real page that echoes the bm-verify parameter", 200,
     page("Example Store", head='<link rel="canonical" href="https://www.example.com/us/?bm-verify=EXAMPLE">')),
    ("a real maintenance page (no geo wall)", 200,
     "<html><head><title>Site Maintenance</title></head><body><header>Example</header><main><h1>We'll be "
     "back soon</h1><p>Our store is down for scheduled maintenance.</p></main><footer>x</footer></body></html>"),
    ("a real page with the perimeterx sensor", 200,
     page("Shoes | Example", head="<script>window._pxAppId='PXexample1';</script>"
                                  '<script src="//client.perimeterx.net/PXexample1/main.min.js"></script>')),
    ("a real page served with 202", 202, page("Order received")),
    ("a real product page with an imperva script", 200,
     page("Widget | Example", head='<script src="/Example-Words-Joined-By-Dashes?d=www.example.com" async>'
                                   "</script>")),
    ("a real page with cloudflare's detections tag", 200, page("Home", body=JSD)),
]


@pytest.mark.parametrize("name,status,body", NOT_BLOCKED, ids=[c[0] for c in NOT_BLOCKED])
def test_real_pages_stay_unblocked(name, status, body):
    assert is_blocked(status, body, content_type=HTML) is False
    assert diagnose(status, body, HTML).ok
