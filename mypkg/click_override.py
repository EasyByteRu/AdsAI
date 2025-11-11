# mypkg/click_override.py
from ads_ai.plan.schema import StepType
def setup(reg):
    def wrap(base):
        def _wrapped(ctx, step):
            reg.trace and reg.trace.write({"event":"plug.click.try"})
            ok = base(ctx, step)
            reg.trace and reg.trace.write({"event":"plug.click.done", "ok": ok})
            return ok
        return _wrapped
    reg.add_step_wrapper(wrap, step=StepType.CLICK)
