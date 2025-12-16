package com.beynd.platform.context;

public final class BeyndContextHolder {

    private static final ThreadLocal<BeyndContext> CONTEXT =
            new InheritableThreadLocal<>();

    private BeyndContextHolder() {}

    public static void set(BeyndContext context) {
        CONTEXT.set(context);
    }

    public static BeyndContext get() {
        return CONTEXT.get();
    }

    public static void clear() {
        CONTEXT.remove();
    }

    public static boolean isPresent() {
        return CONTEXT.get() != null;
    }

    public static BeyndContext inject(BeyndContext newContext) {
        BeyndContext previous = CONTEXT.get();
        CONTEXT.set(newContext);
        return previous;
    }

    public static void restore(BeyndContext previous) {
        if (previous == null) {
            CONTEXT.remove();
        } else {
            CONTEXT.set(previous);
        }
    }
}
