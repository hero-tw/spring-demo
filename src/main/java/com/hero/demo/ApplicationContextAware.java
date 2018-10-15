package com.hero.demo;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;

public class ApplicationContextAware implements org.springframework.context.ApplicationContextAware {

    private static ApplicationContext context;

    public static ApplicationContext getContext() {
        return context;
    }

    @Override
    public void setApplicationContext(ApplicationContext context) throws BeansException {
        ApplicationContextAware.context = context;
    }
}