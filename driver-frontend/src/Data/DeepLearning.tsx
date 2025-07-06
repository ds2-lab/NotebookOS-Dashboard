interface DeepLearningModel {
    category: 'Computer Vision (CV)' | 'Natural Language Processing (NLP)' | 'Speech';
    name: string;
}

interface Dataset {
    category: 'Computer Vision (CV)' | 'Natural Language Processing (NLP)' | 'Speech';
    name: string;
}

export const Categories: ('Computer Vision (CV)' | 'Natural Language Processing (NLP)' | 'Speech')[] = [
    'Computer Vision (CV)',
    'Natural Language Processing (NLP)',
    'Speech',
];

export function GetDatasetCategory(datasetName: string): string {
    switch (datasetName) {
        case 'CIFAR-10':
            return 'Computer Vision (CV)';
        case 'CIFAR-100':
            return 'Computer Vision (CV)';
        case 'Tiny ImageNet':
            return 'Computer Vision (CV)';
        case 'Corpus of Linguistic Acceptability (CoLA)':
            return 'Natural Language Processing (NLP)';
        case 'Truncated IMDb Large Movie Review Dataset (Truncated IMDb)':
            return 'Natural Language Processing (NLP)';
        case 'IMDb Large Movie Review Dataset (IMDb)':
            return 'Natural Language Processing (NLP)';
        case 'LibriSpeech':
            return 'Speech';
    }

    return 'N/A';
}

export function GetModelCategory(modelName: string): string {
    switch (modelName) {
        case 'ResNet-18':
            return 'Computer Vision (CV)';
        case 'VGG-11':
            return 'Computer Vision (CV)';
        case 'VGG-13':
            return 'Computer Vision (CV)';
        case 'VGG-16':
            return 'Computer Vision (CV)';
        case 'VGG-19':
            return 'Inception v3';
        case 'BERT':
            return 'Natural Language Processing (NLP)';
        case 'GPT-2':
            return 'Natural Language Processing (NLP)';
        case 'Deep Speech 2':
            return 'Speech';
    }

    return 'N/A';
}

export const Datasets: Dataset[] = [
    {
        name: 'CIFAR-10',
        category: 'Computer Vision (CV)',
    },
    {
        name: 'CIFAR-100',
        category: 'Computer Vision (CV)',
    },
    {
        name: 'Tiny ImageNet',
        category: 'Computer Vision (CV)',
    },
    {
        name: 'Corpus of Linguistic Acceptability (CoLA)',
        category: 'Natural Language Processing (NLP)',
    },
    {
        name: 'Truncated IMDb Large Movie Review Dataset (Truncated IMDb)',
        category: 'Natural Language Processing (NLP)',
    },
    {
        name: 'IMDb Large Movie Review Dataset (IMDb)',
        category: 'Natural Language Processing (NLP)',
    },
    {
        name: 'LibriSpeech',
        category: 'Speech',
    },
];

export const ComputerVisionDatasets: Dataset[] = [
    {
        name: 'CIFAR-10',
        category: 'Computer Vision (CV)',
    },
    {
        name: 'CIFAR-100',
        category: 'Computer Vision (CV)',
    },
    {
        name: 'Tiny ImageNet',
        category: 'Computer Vision (CV)',
    },
];

export const NLPDatasets: Dataset[] = [
    {
        name: 'Corpus of Linguistic Acceptability (CoLA)',
        category: 'Natural Language Processing (NLP)',
    },
    {
        name: 'Truncated IMDb Large Movie Review Dataset (Truncated IMDb)',
        category: 'Natural Language Processing (NLP)',
    },
    {
        name: 'IMDb Large Movie Review Dataset (IMDb)',
        category: 'Natural Language Processing (NLP)',
    },
];

export const SpeechDatasets: Dataset[] = [
    {
        name: 'LibriSpeech',
        category: 'Speech',
    },
];

export const Models: DeepLearningModel[] = [
    {
        name: 'ResNet-18',
        category: 'Computer Vision (CV)',
    },
    {
        name: 'VGG-11',
        category: 'Computer Vision (CV)',
    },
    {
        name: 'VGG-13',
        category: 'Computer Vision (CV)',
    },
    {
        name: 'VGG-16',
        category: 'Computer Vision (CV)',
    },
    {
        name: 'VGG-19',
        category: 'Computer Vision (CV)',
    },
    {
        name: 'Inception v3',
        category: 'Computer Vision (CV)',
    },
    {
        name: 'BERT',
        category: 'Natural Language Processing (NLP)',
    },
    {
        name: 'GPT-2',
        category: 'Natural Language Processing (NLP)',
    },
    {
        name: 'Deep Speech 2',
        category: 'Speech',
    },
];

export const NLPModels: DeepLearningModel[] = [
    {
        name: 'BERT',
        category: 'Natural Language Processing (NLP)',
    },
    {
        name: 'GPT-2',
        category: 'Natural Language Processing (NLP)',
    },
];

export const SpeechModels: DeepLearningModel[] = [
    {
        name: 'Deep Speech 2',
        category: 'Speech',
    },
];

export const ComputerVisionModels: DeepLearningModel[] = [
    {
        name: 'ResNet-18',
        category: 'Computer Vision (CV)',
    },
    {
        name: 'VGG-11',
        category: 'Computer Vision (CV)',
    },
    {
        name: 'VGG-13',
        category: 'Computer Vision (CV)',
    },
    {
        name: 'VGG-16',
        category: 'Computer Vision (CV)',
    },
    {
        name: 'VGG-19',
        category: 'Computer Vision (CV)',
    },
    {
        name: 'Inception v3',
        category: 'Computer Vision (CV)',
    },
];

export type { DeepLearningModel as DeepLearningModel };
export type { Dataset as Dataset };
