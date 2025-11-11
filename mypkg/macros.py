# mypkg/macros.py
def setup(reg):
    def m_login(node, ctx):
        return [
          {"type":"wait_visible", "selector":"#login"},
          {"type":"input", "selector":"#user", "text":"${username}"},
          {"type":"input", "selector":"#pass", "text":"${password}"},
          {"type":"click", "selector":"text=Sign in"},
          {"type":"wait_url", "pattern":"/dashboard"}
        ]
    reg.register_macro("login_if_needed", m_login)
