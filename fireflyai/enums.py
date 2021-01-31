import os
from enum import Enum

script_dir = os.path.dirname(__file__)


class Pipeline(Enum):
    DATA_CLEANING_POST_IMPUTATION = 'data_cleaning_post_imputation'
    TIME_SERIES_AUTO_FEATURES = 'time_series_auto_features'
    FEATURE_EMBEDDING = 'feature_embedding'
    FEATURE_SELECTION = 'feature_selection'
    DATA_CLEANING_PRE_IMPUTATION = 'data_cleaning_pre_imputation'
    IMPUTATION = 'imputation'
    TEXT_PREPROCESSING = 'text_preprocessing'
    FEATURE_ENGINEERING = 'feature_engineering'
    FEATURE_STACKING = 'feature_stacking'
    ESTIMATOR = 'estimator'
    BALANCING = 'balancing'
    AUTO_SAMPLE_GENERATION = 'auto_sample_generation'

    @staticmethod
    def ALL_CLASSIFICATION():
        return [Pipeline.DATA_CLEANING_PRE_IMPUTATION, Pipeline.TEXT_PREPROCESSING, Pipeline.IMPUTATION,
                Pipeline.DATA_CLEANING_POST_IMPUTATION, Pipeline.AUTO_SAMPLE_GENERATION, Pipeline.BALANCING,
                Pipeline.FEATURE_ENGINEERING, Pipeline.FEATURE_STACKING, Pipeline.FEATURE_EMBEDDING,
                Pipeline.FEATURE_SELECTION, Pipeline.ESTIMATOR]

    @staticmethod
    def ALL_CLASSIFICATION_TIMESERIES():
        return [Pipeline.DATA_CLEANING_PRE_IMPUTATION, Pipeline.TEXT_PREPROCESSING, Pipeline.IMPUTATION,
                Pipeline.DATA_CLEANING_POST_IMPUTATION, Pipeline.BALANCING, Pipeline.FEATURE_ENGINEERING,
                Pipeline.FEATURE_STACKING, Pipeline.FEATURE_EMBEDDING, Pipeline.FEATURE_SELECTION, Pipeline.ESTIMATOR]

    @staticmethod
    def ALL_MULTIVARIATE_TIMESERIES():
        return [Pipeline.DATA_CLEANING_PRE_IMPUTATION, Pipeline.TEXT_PREPROCESSING, Pipeline.IMPUTATION,
                Pipeline.DATA_CLEANING_POST_IMPUTATION, Pipeline.TIME_SERIES_AUTO_FEATURES,
                Pipeline.FEATURE_ENGINEERING, Pipeline.FEATURE_STACKING, Pipeline.FEATURE_EMBEDDING,
                Pipeline.FEATURE_SELECTION, Pipeline.ESTIMATOR]

    @staticmethod
    def ALL_ANOMALY():
        return [Pipeline.DATA_CLEANING_PRE_IMPUTATION, Pipeline.TEXT_PREPROCESSING, Pipeline.IMPUTATION,
                Pipeline.DATA_CLEANING_POST_IMPUTATION, Pipeline.AUTO_SAMPLE_GENERATION, Pipeline.BALANCING,
                Pipeline.FEATURE_ENGINEERING, Pipeline.FEATURE_STACKING, Pipeline.FEATURE_EMBEDDING,
                Pipeline.FEATURE_SELECTION, Pipeline.ESTIMATOR]

    @staticmethod
    def ALL_REGRESSION():
        return [Pipeline.DATA_CLEANING_PRE_IMPUTATION, Pipeline.TEXT_PREPROCESSING, Pipeline.IMPUTATION,
                Pipeline.DATA_CLEANING_POST_IMPUTATION, Pipeline.AUTO_SAMPLE_GENERATION, Pipeline.FEATURE_ENGINEERING,
                Pipeline.FEATURE_STACKING, Pipeline.FEATURE_EMBEDDING, Pipeline.FEATURE_SELECTION, Pipeline.ESTIMATOR]

    @staticmethod
    def ALL_REGRESSION_TIMESERIES():
        return [Pipeline.DATA_CLEANING_PRE_IMPUTATION, Pipeline.TEXT_PREPROCESSING, Pipeline.IMPUTATION,
                Pipeline.DATA_CLEANING_POST_IMPUTATION, Pipeline.TIME_SERIES_AUTO_FEATURES,
                Pipeline.FEATURE_ENGINEERING, Pipeline.FEATURE_STACKING, Pipeline.FEATURE_EMBEDDING,
                Pipeline.FEATURE_SELECTION, Pipeline.ESTIMATOR]


class ProblemType(Enum):
    CLASSIFICATION = 'classification'
    REGRESSION = 'regression'
    ANOMALY_DETECTION = 'anomaly_detection'
    TIMESERIES_CLASSIFICATION = 'classification_timeseries'
    TIMESERIES_REGRESSION = 'regression_timeseries'
    TIMESERIES_ANOMALY_DETECTION = 'anomaly_timeseries'


class Estimator(Enum):
    LIGHT_GRADIENT_BOOSTING = 'light_gradient_boosting'
    LIBSVM_SVR = 'libsvm_svr'
    ADABOOST = 'adaboost'
    XGRADIENT_BOOSTING = 'xgradient_boosting'
    RANSAC = 'ransac'
    LARS = 'lars'
    LIBLINEAR_SVR = 'liblinear_svr'
    K_NEAREST_NEIGHBORS = 'k_nearest_neighbors'
    RANDOM_FOREST = 'random_forest'
    BERNOULLI_NB = 'bernoulli_nb'
    LOGREG = 'logreg'
    CAT_BOOST = 'cat_boost'
    ANOMALY_HIST = 'anomaly_hist'
    BAYESIAN_RIDGE = 'bayesian_ridge'
    GRADIENT_BOOSTING = 'gradient_boosting'
    GAUSSIAN_PROCESS = 'gaussian_process'
    LIBLINEAR_SVC = 'liblinear_svc'
    MULTINOMIAL_NB = 'multinomial_nb'
    DECISION_TREE = 'decision_tree'
    EXPONENTIAL_SMOOTHING = 'exponential_smoothing'
    BART = 'bart'
    ANOMALY_GMM = 'anomaly_gmm'
    NN_KERAS_SEQUENTIAL = 'nn_keras_sequential'
    LDA = 'lda'
    PROJ_LOGIT = 'proj_logit'
    ELASTIC_NET = 'elastic_net'
    ARIMA = 'arima'
    SGD = 'sgd'
    QDA = 'qda'
    LIBSVM_SVC = 'libsvm_svc'
    EXTRA_TREES = 'extra_trees'
    COMPLEMENT_NB = 'complement_nb'
    RIDGE_REGRESSION = 'ridge_regression'
    GAUSSIAN_NB = 'gaussian_nb'
    RIDGE_CLASSIFICATION = 'ridge_classification'
    AVERAGE_ESTIMATOR = 'average_estimator'
    ANOMALY_ISOF = 'anomaly_isof'
    PASSIVE_AGGRESSIVE = 'passive_aggressive'

    @staticmethod
    def ALL_CLASSIFICATION():
        return [Estimator.RANDOM_FOREST, Estimator.XGRADIENT_BOOSTING, Estimator.ADABOOST, Estimator.EXTRA_TREES,
                Estimator.K_NEAREST_NEIGHBORS, Estimator.PASSIVE_AGGRESSIVE, Estimator.PROJ_LOGIT,
                Estimator.BERNOULLI_NB, Estimator.NN_KERAS_SEQUENTIAL, Estimator.ANOMALY_GMM, Estimator.ANOMALY_ISOF,
                Estimator.ANOMALY_HIST, Estimator.DECISION_TREE, Estimator.GAUSSIAN_NB, Estimator.GRADIENT_BOOSTING,
                Estimator.SGD, Estimator.QDA, Estimator.MULTINOMIAL_NB, Estimator.LOGREG, Estimator.LIBSVM_SVC,
                Estimator.LDA, Estimator.LIBLINEAR_SVC, Estimator.RIDGE_CLASSIFICATION,
                Estimator.LIGHT_GRADIENT_BOOSTING, Estimator.CAT_BOOST, Estimator.COMPLEMENT_NB]

    @staticmethod
    def ALL_CLASSIFICATION_TIMESERIES():
        return [Estimator.RANDOM_FOREST, Estimator.XGRADIENT_BOOSTING, Estimator.ADABOOST, Estimator.EXTRA_TREES,
                Estimator.K_NEAREST_NEIGHBORS, Estimator.PASSIVE_AGGRESSIVE, Estimator.PROJ_LOGIT,
                Estimator.BERNOULLI_NB, Estimator.NN_KERAS_SEQUENTIAL, Estimator.ANOMALY_GMM, Estimator.ANOMALY_ISOF,
                Estimator.ANOMALY_HIST, Estimator.DECISION_TREE, Estimator.GAUSSIAN_NB, Estimator.GRADIENT_BOOSTING,
                Estimator.SGD, Estimator.QDA, Estimator.MULTINOMIAL_NB, Estimator.LOGREG, Estimator.LIBSVM_SVC,
                Estimator.LDA, Estimator.LIBLINEAR_SVC, Estimator.RIDGE_CLASSIFICATION,
                Estimator.LIGHT_GRADIENT_BOOSTING, Estimator.CAT_BOOST, Estimator.COMPLEMENT_NB]

    @staticmethod
    def ALL_MULTIVARIATE_TIMESERIES():
        return [Estimator.RANDOM_FOREST, Estimator.RIDGE_REGRESSION, Estimator.XGRADIENT_BOOSTING, Estimator.ADABOOST,
                Estimator.EXTRA_TREES, Estimator.GAUSSIAN_PROCESS, Estimator.K_NEAREST_NEIGHBORS,
                Estimator.NN_KERAS_SEQUENTIAL, Estimator.DECISION_TREE, Estimator.GRADIENT_BOOSTING,
                Estimator.LIBLINEAR_SVR, Estimator.LIBSVM_SVR, Estimator.SGD, Estimator.LARS, Estimator.ELASTIC_NET,
                Estimator.LIGHT_GRADIENT_BOOSTING, Estimator.CAT_BOOST, Estimator.RANSAC, Estimator.BAYESIAN_RIDGE]

    @staticmethod
    def ALL_ANOMALY():
        return [Estimator.RANDOM_FOREST, Estimator.XGRADIENT_BOOSTING, Estimator.ADABOOST, Estimator.EXTRA_TREES,
                Estimator.K_NEAREST_NEIGHBORS, Estimator.PASSIVE_AGGRESSIVE, Estimator.PROJ_LOGIT,
                Estimator.BERNOULLI_NB, Estimator.NN_KERAS_SEQUENTIAL, Estimator.ANOMALY_GMM, Estimator.ANOMALY_ISOF,
                Estimator.ANOMALY_HIST, Estimator.DECISION_TREE, Estimator.GAUSSIAN_NB, Estimator.GRADIENT_BOOSTING,
                Estimator.SGD, Estimator.QDA, Estimator.MULTINOMIAL_NB, Estimator.LOGREG, Estimator.LIBSVM_SVC,
                Estimator.LDA, Estimator.LIBLINEAR_SVC, Estimator.RIDGE_CLASSIFICATION,
                Estimator.LIGHT_GRADIENT_BOOSTING, Estimator.CAT_BOOST, Estimator.COMPLEMENT_NB]

    @staticmethod
    def ALL_REGRESSION():
        return [Estimator.RANDOM_FOREST, Estimator.RIDGE_REGRESSION, Estimator.XGRADIENT_BOOSTING, Estimator.ADABOOST,
                Estimator.EXTRA_TREES, Estimator.GAUSSIAN_PROCESS, Estimator.K_NEAREST_NEIGHBORS,
                Estimator.NN_KERAS_SEQUENTIAL, Estimator.DECISION_TREE, Estimator.GRADIENT_BOOSTING,
                Estimator.LIBLINEAR_SVR, Estimator.LIBSVM_SVR, Estimator.SGD, Estimator.LARS, Estimator.ELASTIC_NET,
                Estimator.LIGHT_GRADIENT_BOOSTING, Estimator.CAT_BOOST, Estimator.RANSAC, Estimator.BAYESIAN_RIDGE,
                Estimator.BART]

    @staticmethod
    def ALL_REGRESSION_TIMESERIES():
        return [Estimator.RANDOM_FOREST, Estimator.RIDGE_REGRESSION, Estimator.XGRADIENT_BOOSTING, Estimator.ADABOOST,
                Estimator.EXTRA_TREES, Estimator.GAUSSIAN_PROCESS, Estimator.K_NEAREST_NEIGHBORS,
                Estimator.NN_KERAS_SEQUENTIAL, Estimator.DECISION_TREE, Estimator.GRADIENT_BOOSTING,
                Estimator.LIBLINEAR_SVR, Estimator.LIBSVM_SVR, Estimator.SGD, Estimator.LARS, Estimator.ELASTIC_NET,
                Estimator.LIGHT_GRADIENT_BOOSTING, Estimator.CAT_BOOST, Estimator.RANSAC, Estimator.BAYESIAN_RIDGE,
                Estimator.AVERAGE_ESTIMATOR, Estimator.ARIMA, Estimator.EXPONENTIAL_SMOOTHING, Estimator.BART]


class FeatureType(Enum):
    CATEGORICAL = 'categorical'
    NUMERICAL = 'numerical'
    TEXT = 'text'
    DATETIME = 'datetime'


class TargetMetric(Enum):
    F2 = 'f2'
    ACCURACY = 'accuracy'
    MAE = 'mae'
    NORMALIZED_MAE = 'normalized_mae'
    MEDIAN_AE = 'median_ae'
    MAPE = 'mape'
    RMSLE = 'rmsle'
    COST_METRIC = 'cost_metric'
    MSE = 'mse'
    RMSE = 'rmse'
    NORMALIZED_MSE = 'normalized_mse'
    MAE_DISCRETE = 'mae_discrete'
    RMSPE = 'rmspe'
    NORMALIZED_RMSE = 'normalized_rmse'
    RECALL_MACRO = 'recall_macro'
    NORMALIZED_GINI = 'normalized_gini'
    SIGNED_SUM = 'signed_sum'
    F1 = 'f1'
    LOG_LOSS = 'log_loss'
    AUC = 'auc'
    R2 = 'r2'
    R1 = 'r1'
    JACCARD = 'jaccard'
    NORMALIZED_MUTUAL_INFO = 'normalized_mutual_info'

    @staticmethod
    def ALL_CLASSIFICATION():
        return [TargetMetric.F1, TargetMetric.F2, TargetMetric.NORMALIZED_GINI, TargetMetric.AUC, TargetMetric.LOG_LOSS,
                TargetMetric.ACCURACY, TargetMetric.RECALL_MACRO, TargetMetric.JACCARD,
                TargetMetric.NORMALIZED_MUTUAL_INFO, TargetMetric.COST_METRIC]

    @staticmethod
    def ALL_CLASSIFICATION_TIMESERIES():
        return [TargetMetric.F1, TargetMetric.F2, TargetMetric.NORMALIZED_GINI, TargetMetric.AUC, TargetMetric.LOG_LOSS,
                TargetMetric.ACCURACY, TargetMetric.RECALL_MACRO, TargetMetric.JACCARD,
                TargetMetric.NORMALIZED_MUTUAL_INFO, TargetMetric.COST_METRIC]

    @staticmethod
    def ALL_MULTIVARIATE_TIMESERIES():
        return [TargetMetric.RMSE, TargetMetric.R1, TargetMetric.NORMALIZED_MSE, TargetMetric.NORMALIZED_RMSE,
                TargetMetric.MEDIAN_AE, TargetMetric.R2, TargetMetric.MAE, TargetMetric.MAE_DISCRETE,
                TargetMetric.NORMALIZED_MAE, TargetMetric.MSE, TargetMetric.RMSPE, TargetMetric.RMSLE,
                TargetMetric.SIGNED_SUM, TargetMetric.MAPE]

    @staticmethod
    def ALL_ANOMALY():
        return [TargetMetric.F1, TargetMetric.F2, TargetMetric.NORMALIZED_GINI, TargetMetric.AUC, TargetMetric.LOG_LOSS,
                TargetMetric.ACCURACY, TargetMetric.RECALL_MACRO, TargetMetric.JACCARD,
                TargetMetric.NORMALIZED_MUTUAL_INFO, TargetMetric.COST_METRIC]

    @staticmethod
    def ALL_REGRESSION():
        return [TargetMetric.RMSE, TargetMetric.R1, TargetMetric.NORMALIZED_MSE, TargetMetric.NORMALIZED_RMSE,
                TargetMetric.MEDIAN_AE, TargetMetric.R2, TargetMetric.MAE, TargetMetric.MAE_DISCRETE,
                TargetMetric.NORMALIZED_MAE, TargetMetric.MSE, TargetMetric.RMSPE, TargetMetric.RMSLE,
                TargetMetric.SIGNED_SUM, TargetMetric.MAPE]

    @staticmethod
    def ALL_REGRESSION_TIMESERIES():
        return [TargetMetric.RMSE, TargetMetric.R1, TargetMetric.NORMALIZED_MSE, TargetMetric.NORMALIZED_RMSE,
                TargetMetric.MEDIAN_AE, TargetMetric.R2, TargetMetric.MAE, TargetMetric.MAE_DISCRETE,
                TargetMetric.NORMALIZED_MAE, TargetMetric.MSE, TargetMetric.RMSPE, TargetMetric.RMSLE,
                TargetMetric.SIGNED_SUM, TargetMetric.MAPE]


class InterpretabilityLevel(Enum):
    EXPLAINABLE = 2
    PRECISE = 0
    STANDARD = 0
    STANDARD_WITH_EXPLAINABILITY = 1
    SIMPLE_WITH_EXPLAINABILITY = 2
    LEAKAGE_DETECTION = 3


class SplittingStrategy(Enum):
    STRATIFIED = 'stratified'
    SHUFFLED = 'shuffled'
    TIME_ORDER = 'time_order'

    @staticmethod
    def ALL_CLASSIFICATION():
        return [SplittingStrategy.STRATIFIED, SplittingStrategy.SHUFFLED, SplittingStrategy.TIME_ORDER]

    @staticmethod
    def ALL_CLASSIFICATION_TIMESERIES():
        return [SplittingStrategy.TIME_ORDER]

    @staticmethod
    def ALL_MULTIVARIATE_TIMESERIES():
        return [SplittingStrategy.TIME_ORDER]

    @staticmethod
    def ALL_ANOMALY():
        return [SplittingStrategy.STRATIFIED, SplittingStrategy.SHUFFLED, SplittingStrategy.TIME_ORDER]

    @staticmethod
    def ALL_REGRESSION():
        return [SplittingStrategy.TIME_ORDER, SplittingStrategy.SHUFFLED]

    @staticmethod
    def ALL_REGRESSION_TIMESERIES():
        return [SplittingStrategy.TIME_ORDER]


class ValidationStrategy(Enum):
    HOLDOUT = 'holdout'
    CROSS_VALIDATION = 'cv'


class CVStrategy(Enum):
    AVERAGE_MODELS = 'average_models'
    LAST_MODEL = 'last_model'

