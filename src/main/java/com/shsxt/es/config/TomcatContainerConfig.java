package com.shsxt.es.config;


import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * @author qiong.peng
 * @Date 2019/10/12
 */

@Configuration
public class TomcatContainerConfig {
    @Value("${tldSkipPatterns}")
    private String[] tldSkipPatterns;

    @Bean
    public BeanPostProcessor TomcatContainerPostProcessor() {
        List<String> notEmptyTldSkipPatterns = Arrays.stream(tldSkipPatterns)
                .filter(tldSkipPattern -> !tldSkipPattern.trim().isEmpty())
                .collect(toList());


        return new BeanPostProcessor() {
            @Override
            public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
                return bean;
            }

            @Override
            public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
                if (beanName.equals("tomcatEmbeddedServletContainerFactory") &&
                        bean instanceof TomcatEmbeddedServletContainerFactory) {
                    TomcatEmbeddedServletContainerFactory factory = (TomcatEmbeddedServletContainerFactory) bean;
                    if (!notEmptyTldSkipPatterns.isEmpty()) {
                        factory.addTldSkipPatterns(notEmptyTldSkipPatterns.toArray(new String[0]));
                    }
                }
                return bean;
            }
        };
    }
}
