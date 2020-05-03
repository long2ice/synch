from mysql2ch import settings
from mysql2ch.ui.main import app


def run_app(args):
    if not settings.UI_ENABLE:
        raise Exception('settings.UI_ENABLE is not True!')
    app.run(host=args.host, port=args.port, debug=settings.DEBUG)
