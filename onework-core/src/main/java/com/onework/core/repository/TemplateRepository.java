package com.onework.core.repository;

import com.onework.core.entity.Template;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * @author kangj
 * @date 2020/11/20
 **/
@Repository
public interface TemplateRepository extends JpaRepository<Template, String> {
}
