-- -----------------------------------------------------
-- Table `kubernetes`.`Affinity`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `kubernetes`.`Affinity` (
  `uid` TEXT NOT NULL,
  `name` TEXT NOT NULL,
  `namespace` TEXT NOT NULL,
  `weight` INT NOT NULL,
  `affinity` TEXT NOT NULL,
  `created_at` DATETIME NOT NULL)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `kubernetes`.`Container`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `kubernetes`.`Container` (
  `pod_uid` TEXT NOT NULL,
  `pod` TEXT NOT NULL,
  `namespace` TEXT NOT NULL,
  `container` TEXT NOT NULL,
  `limit_memory` BIGINT NOT NULL,
  `limit_cpu` BIGINT NOT NULL,
  `limit_disk` BIGINT NOT NULL,
  `request_memory` BIGINT NOT NULL,
  `request_cpu` BIGINT NOT NULL,
  `request_disk` BIGINT NOT NULL,
  `created_at` DATETIME NOT NULL)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `kubernetes`.`Endpoint`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `kubernetes`.`Endpoint` (
  `uid` TEXT NOT NULL,
  `name` TEXT NOT NULL,
  `namespace` TEXT NOT NULL,
  `hostname` TEXT NOT NULL,
  `ip` TEXT NOT NULL,
  `portname` TEXT NOT NULL,
  `port` INT NOT NULL,
  `created_at` DATETIME NOT NULL)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `kubernetes`.`Node`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `kubernetes`.`Node` (
  `uid` TEXT NOT NULL,
  `name` TEXT NOT NULL,
  `namespace` TEXT NOT NULL,
  `free_memory` BIGINT NOT NULL,
  `free_cpu` BIGINT NOT NULL,
  `free_disk` BIGINT NOT NULL,
  `capacity_memory` BIGINT NOT NULL,
  `capacity_cpu` BIGINT NOT NULL,
  `capacity_disk` BIGINT NOT NULL,
  `created_at` DATETIME NOT NULL)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `kubernetes`.`Node_Affinity`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `kubernetes`.`Node_Affinity` (
  `uid` TEXT NOT NULL,
  `name` TEXT NOT NULL,
  `namespace` TEXT NOT NULL,
  `weight` INT NOT NULL,
  `affinity` TEXT NOT NULL,
  `created_at` DATETIME NOT NULL)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `kubernetes`.`Node_Metrics`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `kubernetes`.`Node_Metrics` (
  `name` TEXT NOT NULL,
  `namespace` TEXT NOT NULL,
  `window` BIGINT NOT NULL,
  `usage_memory` BIGINT NOT NULL,
  `usage_cpu` BIGINT NOT NULL,
  `usage_disk` BIGINT NOT NULL,
  `created_at` DATETIME NOT NULL)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `kubernetes`.`Pod`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `kubernetes`.`Pod` (
  `uid` TEXT NOT NULL,
  `name` TEXT NOT NULL,
  `namespace` TEXT NOT NULL,
  `application` TEXT NOT NULL,
  `deployment` TEXT NOT NULL,
  `node` TEXT NOT NULL,
  `ip` TEXT NOT NULL,
  `created_at` DATETIME NOT NULL,
  PRIMARY KEY (`uid`, `name`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `kubernetes`.`Pod_Metrics`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `kubernetes`.`Pod_Metrics` (
  `pod` TEXT NOT NULL,
  `container` TEXT NOT NULL,
  `namespace` TEXT NOT NULL,
  `window` BIGINT NOT NULL,
  `usage_memory` BIGINT NOT NULL,
  `usage_cpu` BIGINT NOT NULL,
  `usage_disk` BIGINT NOT NULL,
  `created_at` DATETIME NOT NULL)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `kubernetes`.`Traffic`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `kubernetes`.`Traffic` (
  `src_deployment` TEXT NOT NULL,
  `src_namespace` TEXT NOT NULL,
  `dst_deployment` TEXT NOT NULL,
  `dst_pod` TEXT NOT NULL,
  `dst_instance` TEXT NOT NULL,
  `dst_service` TEXT NOT NULL,
  `dst_namespace` TEXT NOT NULL,
  `protocol` TEXT NOT NULL,
  `http_status_code` INT NOT NULL,
  `grpc_status_code` INT NOT NULL,
  `metric` TEXT NOT NULL,
  `value` DOUBLE NOT NULL)
ENGINE = InnoDB;

