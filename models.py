from pydantic import BaseModel, HttpUrl, RootModel
from typing import List, Optional, Dict, Any


class Category(BaseModel):
    id: int
    name: str
    slug: str
    rootId: int
    compare: bool
    pageRootId: int | None
    specification: str | None = None


class Location(BaseModel):
    id: int
    name: str
    namePrepositional: str
    isCurrent: bool
    isRegion: bool


class AddressDetailed(BaseModel):
    locationName: str


class PriceDetailed(BaseModel):
    enabled: bool
    fullString: str
    hasValue: bool
    postfix: str
    string: str
    stringWithoutDiscount: Optional[str]
    title: Dict[str, str]
    titleDative: str
    value: int
    wasLowered: bool
    exponent: str


class Image(RootModel):
    root: Dict[str, HttpUrl]


class Geo(BaseModel):
    geoReferences: List[Any]
    formattedAddress: str


class Contacts(BaseModel):
    phone: bool
    delivery: bool
    message: bool
    messageTitle: str
    action: str
    onModeration: bool
    hasCVPackage: bool
    hasEmployeeBalanceForCv: bool
    serviceBooking: bool


class Gallery(BaseModel):
    alt: str | None = None
    cropImagesInfo: Optional[Any]
    extraPhoto: Optional[Any]
    hasLeadgenOverlay: bool
    has_big_image: bool
    imageAlt: str
    imageLargeUrl: str
    imageLargeVipUrl: str
    imageUrl: str
    imageVipUrl: str
    image_large_urls: List[Any]
    image_urls: List[Any]
    images: List[Any]
    imagesCount: int
    isFirstImageHighImportance: bool
    isLazy: bool
    noPhoto: bool
    showSlider: bool
    wideSnippetUrls: List[Any]


class UserLogo(BaseModel):
    link: str | None = None
    src: HttpUrl | str | None = None
    developerId: Optional[int]


class IvaComponent(BaseModel):
    component: str
    payload: Optional[Dict[str, Any]] = None


class IvaStep(BaseModel):
    componentData: IvaComponent
    payload: Optional[Dict[str, Any]] = None
    default: bool


class AnswerOnReview(BaseModel):
    text: str | None = None
    author: str | None = None


class Review(BaseModel):
    author: str | None = None
    text: str | None = None
    date: str | None = None
    score: float | None = None
    answer: AnswerOnReview | None = None

class Seller(BaseModel):
    url: str | None = None
    name: str | None = None
    type: str | None = None
    registration_date: str | None = None
    active_ad_count: int = 0
    completed_ad_count: int = 0
    characteristics: str | None = None
    experience: str | None = None


class Item(BaseModel):
    id: int | dict | None = None
    categoryId: int | dict | None = None
    locationId: int | dict | None = None
    isVerifiedItem: bool | None = None
    urlPath: str | None = None
    title: str | None = None
    description: str | None = None
    additional_info: str | None = None
    price_list: str | None = None
    category: Category | None = None
    location: Location | None = None
    addressDetailed: AddressDetailed | None = None
    sortTimeStamp: int | None = None
    turnOffDate: bool | None = None
    priceDetailed: PriceDetailed | None = None
    normalizedPrice: Optional[str] | None = None
    priceWithoutDiscount: Optional[str] | None = None
    discountPercent: Optional[int] | Optional[str] | None = None
    lastMinuteOffer: Optional[str] | Optional[dict] | None = None
    images: List[Image] | None = None
    imagesCount: int | None = None
    videos: List[str] = []
    isFavorite: bool | None = None
    isNew: bool | None = None
    geo: Geo | None = None
    phoneImage: Optional[str] | None = None
    cvViewed: bool | None = None
    isXl: bool | None = None
    hasFooter: bool | None = None
    contacts: Contacts | None = None
    gallery: Gallery | None = None
    loginLink: str | None = None
    authLink: str | None = None
    userLogo: UserLogo | None = None
    isMarketplace: bool | None = None
    iva: Dict[str, List[IvaStep]] | None = None
    hasVideo: bool | None = None
    hasRealtyLayout: bool | None = None
    coords: Dict[str, Any] | None = None
    groupData: Optional[Any] | None = None
    isReMapPromotion: bool | None = None
    isReserved: bool | None = None
    type: str | None = None
    ratingExperimentGroup: str | None = None
    isRatingExperiment: bool | None = None
    closedItemsText: str | None = None
    closestAddressId: int | None = None
    isSparePartsCompatibility: bool | None = None
    sellerId: str | None = None
    seller: Seller | None = None
    isPromotion: bool = False
    total_views: int | None = None
    today_views: int | None = None
    phone: str | None = None
    reviews: str | None = None
    count_reviews: int = 0
    score: float | None = None


class ItemsResponse(BaseModel):
    items: List[Item]


