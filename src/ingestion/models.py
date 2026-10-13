from pydantic import BaseModel, ConfigDict, Field


class _ApiModel(BaseModel):
    # Keep unknown API fields so nothing is lost when we later store raw payloads.
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class CardSet(_ApiModel):
    id: str
    name: str
    series: str | None = None
    printed_total: int | None = Field(default=None, alias="printedTotal")
    total: int | None = None
    ptcgo_code: str | None = Field(default=None, alias="ptcgoCode")
    release_date: str | None = Field(default=None, alias="releaseDate")
    updated_at: str | None = Field(default=None, alias="updatedAt")


class CardImages(_ApiModel):
    small: str | None = None
    large: str | None = None


class Card(_ApiModel):
    id: str
    name: str
    supertype: str | None = None
    subtypes: list[str] = Field(default_factory=list)
    hp: str | None = None
    types: list[str] = Field(default_factory=list)
    card_set: CardSet | None = Field(default=None, alias="set")
    number: str | None = None
    artist: str | None = None
    rarity: str | None = None
    national_pokedex_numbers: list[int] = Field(
        default_factory=list, alias="nationalPokedexNumbers"
    )
    images: CardImages | None = None


class _PageMeta(_ApiModel):
    page: int = 1
    page_size: int = Field(alias="pageSize")
    count: int = 0
    total_count: int = Field(alias="totalCount")


class CardPage(_PageMeta):
    data: list[Card] = Field(default_factory=list)


class SetPage(_PageMeta):
    data: list[CardSet] = Field(default_factory=list)
