from enum import Enum


class ProblemType(Enum):
    CLASSIFICATION = 'classification'
    REGRESSION = 'regression'
    ANOMALY_DETECTION = 'anomaly_detection'
    TIMESERIES_CALSSIFICATION = 'classification_timeseries'
    TIMESERIES_REGRESSION = 'regression_timeseries'
    TIMESERIES_ANOMALY_DETECTION = 'anomaly_timeseries'


class ColumnType(Enum):
    CATEGORICAL = 'categorical'
    NUMERICAL = 'numerical'
    TEXT = 'text'
    DATETIME = 'datetime'


class TargetMetric(Enum):
    RECALL_MACRO = 'recall_macro'
    F1 = 'f1'
    F2 = 'f2'
    ACCURACY = 'accuracy'
    NORMALIZED_GINI = 'normalized_gini'
    AUC = 'auc'
    LOG_LOSS = 'log_loss'
    JACARD = 'jacard'
    NORMALIZED_MUTUAL_INFO = 'normalized_mutual_info'
    COST_METRIC = 'cost_metric'


class InterpretabilityLevel(Enum):
    PRECISE = 'Precise'
    EXPLAINABLE = 'Explainable'


class Estimator(Enum):
    RANDOM_FOREST = 'random_forest'
    EXTRA_TREES = 'extra_trees'
    DECISION_TREE = 'decision_tree'
    XGBOOST = 'xgboost'
    LIGHT_GRADIENT_BOOSTING = 'light_gradient_boosting'
    CAT_BOOST = 'cat_boost'
    ADA_BOOST = 'ada_boost'
    GRADIENT_BOOSTING = 'gradient_boosting'
    LOGISTIC_REGRESSION = 'logistic_regression'


class PipelineStep(Enum):
    DATA_CLEANING_PRE_IMPUTATION = 'data_cleaning_pre_imputation'
    DATA_CLEANING_POST_IMPUTATION = 'data_cleaning_post_imputation'
    IMPUTATION = 'imputation'
    TEXT_PREPROCESSING = 'text_preprocessing'
    AUTO_SAMPLE_GENERATION = 'auto_sample_generation'
    BALANCING = 'balancing'
    FEATURE_ENGINEERING = 'feature_engineering'
    FEATURE_STACKING = 'feature_stacking'
    FEATURE_EMBEDDING = 'feature_embedding'
    FEATURE_SELECTION = 'feature_selection'
    ESTIMATOR = 'estimator'


class SplittingStrategy(Enum):
    STRATIFIED = 'stratified'
