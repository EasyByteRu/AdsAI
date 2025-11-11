# mypkg/step_logger.py
def setup(reg):
    def before(ctx, st, step):  # noqa
        reg.trace and reg.trace.write({"event":"plug.before", "t": st.value})
    def after(ctx, st, step, ok):  # noqa
        reg.trace and reg.trace.write({"event":"plug.after", "t": st.value, "ok": ok})
    reg.add_before_step_hook(before)
    reg.add_after_step_hook(after)
