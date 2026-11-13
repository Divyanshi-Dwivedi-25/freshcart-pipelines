
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from sklearn.metrics import confusion_matrix, accuracy_score, classification_report

@transformer
def evaluate_model(df, *args, **kwargs):
    y_true = df['target']
    y_pred = df['predicted']

    accuracy = accuracy_score(y_true, y_pred)
    cm = confusion_matrix(y_true, y_pred)

    print("Model Evaluation Results")
    print("Accuracy:", round(accuracy, 3))
    print("\nConfusion Matrix:")
    print(cm)

    print("\nClassification Report:")
    print(classification_report(y_true, y_pred))

    return {
        'accuracy': accuracy,
        'confusion_matrix': cm.tolist(),
        'classification_report': classification_report(
            y_true, y_pred, output_dict=True
        )
    }
