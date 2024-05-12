MIN_PROXY_SCORE = 50

CLAIM_REWARDS_ONLY = False  # claim tiers rewards only (https://app.getgrass.io/dashboard/referral-program)

THREADS = 2  # for register account / claim rewards mode only

CHECK_POINTS = True  # show point for each account every nearly 10 minutes

STOP_ACCOUNTS_WHEN_SITE_IS_DOWN = True  # stop account for 20 minutes, to reduce proxy traffic usage

SHOW_LOGS_RARELY = False

CHECK_POINT_ONLY = False

REF_CODE='L0f3w4hDSA2wy1p'


# REGISTER PARAMETRS ONLY
REGISTER_ACCOUNT_ONLY = True
REGISTER_DELAY = (3, 7)

TWO_CAPTCHA_API_KEY = ""
ANTICAPTCHA_API_KEY = ""
CAPMONSTER_API_KEY = ""
CAPSOLVER_API_KEY = ""
CAPTCHAAI_API_KEY = ""

# Captcha params, left empty
CAPTCHA_PARAMS = {
    "captcha_type": "v2",
    "invisible_captcha": False,
    "sitekey": "6LdyCj0pAAAAAFvvSTRHYOzddUPMPcH232u7a9e0",
    "captcha_url": "https://app.getgrass.io/register"
}

########################################

ACCOUNTS_FILE_PATH = "data/accounts.txt"
PROXIES_FILE_PATH = "data/proxies.txt"
WSS_PROXIES_FILE_PATH = "data/wss_proxies.txt"
