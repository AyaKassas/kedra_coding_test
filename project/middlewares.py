#%%
import random

class UserAgentMiddleware:
    """
    Lightweight downloader middleware to rotate User-Agent headers.
    This reduces the chance of being blocked while keeping the setup simple.
    """

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    def process_request(self, request, spider=None):
        request.headers.setdefault("User-Agent", random.choice(self.USER_AGENTS))
