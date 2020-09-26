package com.onework.core;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class ApplicationContextGetter implements ApplicationContextAware {
    private static ApplicationContext context;

    public static ApplicationContext getContext() {
        return context;
    }

    private static void setContext(ApplicationContext context) {
        ApplicationContextGetter.context = context;
    }

    @Override
    public void setApplicationContext(ApplicationContext context) {
        setContext(context);
    }
}
