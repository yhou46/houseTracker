import unittest
import usaddress # type: ignore[import-untyped]
from datetime import datetime, timezone
from decimal import Decimal

from shared.iproperty import (
    IPropertyHistory,
    IPropertyHistoryEvent,
    PropertyHistoryEventType,
    IPropertyMetadata,
    PropertyArea,
    PropertyType,
    PropertyStatus,
    IPropertyDataSource,
    AreaUnit,
    )
from shared.iproperty_address import IPropertyAddress, get_address_components

class Test_get_address(unittest.TestCase):
    def test_street_address(self) -> None:
        # Map from full streed address to full street address
        testCases = {
            "1838 Market St,Kirkland, WA 98033" : "1838 Market St",
            "Apt 116, 6910 Old Redmond Rd, Redmond, WA, 98052" : "6910 Old Redmond Rd",
            "655 Crockett St Unit A101,Seattle, WA 98109": "655 Crockett St",
            "6928 155th Pl SE,Snohomish, WA 98296": "6928 155th Pl SE", # These 2 should be the same but test fails
            "6928 155th Place SE, Snohomish, WA 98296": "6928 155th Pl SE",
        }

        for fullAddress, expectedStreetAddress in testCases.items():
            components = get_address_components(fullAddress)
            streetAddress = components["street"]
            self.assertEqual(expectedStreetAddress, streetAddress)

    def test_unit(self) -> None:
        # Map from full streed address to full street address
        testCases = {
            "1838 Market St,Kirkland, WA 98033" : "",
            "Apt 116, 6910 Old Redmond Rd, Redmond, WA, 98052" : "APT 116",
            "655 Crockett St Unit A101,Seattle, WA 98109": "APT A101",
        }

        for fullAddress, expectedStreetAddress in testCases.items():
            components = get_address_components(fullAddress)
            unit  = components.get("unit", "")
            self.assertEqual(expectedStreetAddress, unit)

class Test_IPropertyMetadata(unittest.TestCase):

    def setUp(self) -> None:
        super().setUp()
        # Create shared test data
        self.test_address = IPropertyAddress("1838 Market St,Kirkland, WA 98033")
        self.test_area = PropertyArea(Decimal("2000"), AreaUnit.SquareFeet)
        self.test_lot_area = PropertyArea(Decimal("0.25"), AreaUnit.Acres)
        self.test_property_type = PropertyType.SingleFamily
        self.test_bedrooms = Decimal("3")
        self.test_bathrooms = Decimal("2.5")
        self.test_year_built = 2010
        self.test_status = PropertyStatus.Active
        self.test_price = Decimal("750000")
        self.test_last_updated = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        self.test_data_sources = [
            IPropertyDataSource("redfin_123", "https://redfin.com/property/123", "Redfin"),
            IPropertyDataSource("zillow_456", "https://zillow.com/property/456", "Zillow")
        ]

        # Create base metadata object
        self.base_metadata = IPropertyMetadata(
            address=self.test_address,
            area=self.test_area,
            property_type=self.test_property_type,
            lot_area=self.test_lot_area,
            number_of_bedrooms=self.test_bedrooms,
            number_of_bathrooms=self.test_bathrooms,
            year_built=self.test_year_built,
            status=self.test_status,
            price=self.test_price,
            last_updated=self.test_last_updated,
            data_sources=self.test_data_sources
        )

    def test_eq_identical_metadata(self) -> None:
        """Test that identical metadata objects are equal."""
        metadata2 = IPropertyMetadata(
            address=self.test_address,
            area=self.test_area,
            property_type=self.test_property_type,
            lot_area=self.test_lot_area,
            number_of_bedrooms=self.test_bedrooms,
            number_of_bathrooms=self.test_bathrooms,
            year_built=self.test_year_built,
            status=self.test_status,
            price=self.test_price,
            last_updated=self.test_last_updated,
            data_sources=self.test_data_sources
        )
        self.assertEqual(self.base_metadata, metadata2)

    def test_eq_different_address(self) -> None:
        """Test that metadata with different addresses are not equal."""
        different_address = IPropertyAddress("456 Oak Ave,Redmond, WA 98052")
        metadata2 = IPropertyMetadata(
            address=different_address,
            area=self.test_area,
            property_type=self.test_property_type,
            lot_area=self.test_lot_area,
            number_of_bedrooms=self.test_bedrooms,
            number_of_bathrooms=self.test_bathrooms,
            year_built=self.test_year_built,
            status=self.test_status,
            price=self.test_price,
            last_updated=self.test_last_updated,
            data_sources=self.test_data_sources
        )
        self.assertNotEqual(self.base_metadata, metadata2)

    def test_eq_different_area(self) -> None:
        """Test that metadata with different areas are not equal."""
        different_area = PropertyArea(Decimal("2500"), AreaUnit.SquareFeet)
        metadata2 = IPropertyMetadata(
            address=self.test_address,
            area=different_area,
            property_type=self.test_property_type,
            lot_area=self.test_lot_area,
            number_of_bedrooms=self.test_bedrooms,
            number_of_bathrooms=self.test_bathrooms,
            year_built=self.test_year_built,
            status=self.test_status,
            price=self.test_price,
            last_updated=self.test_last_updated,
            data_sources=self.test_data_sources
        )
        self.assertNotEqual(self.base_metadata, metadata2)

    def test_eq_different_property_type(self) -> None:
        """Test that metadata with different property types are not equal."""
        metadata2 = IPropertyMetadata(
            address=self.test_address,
            area=self.test_area,
            property_type=PropertyType.Condo,
            lot_area=self.test_lot_area,
            number_of_bedrooms=self.test_bedrooms,
            number_of_bathrooms=self.test_bathrooms,
            year_built=self.test_year_built,
            status=self.test_status,
            price=self.test_price,
            last_updated=self.test_last_updated,
            data_sources=self.test_data_sources
        )
        self.assertNotEqual(self.base_metadata, metadata2)

    def test_eq_different_status(self) -> None:
        """Test that metadata with different status are not equal."""
        metadata2 = IPropertyMetadata(
            address=self.test_address,
            area=self.test_area,
            property_type=self.test_property_type,
            lot_area=self.test_lot_area,
            number_of_bedrooms=self.test_bedrooms,
            number_of_bathrooms=self.test_bathrooms,
            year_built=self.test_year_built,
            status=PropertyStatus.Sold,
            price=self.test_price,
            last_updated=self.test_last_updated,
            data_sources=self.test_data_sources
        )
        self.assertNotEqual(self.base_metadata, metadata2)

    def test_eq_different_price(self) -> None:
        """Test that metadata with different prices are not equal."""
        metadata2 = IPropertyMetadata(
            address=self.test_address,
            area=self.test_area,
            property_type=self.test_property_type,
            lot_area=self.test_lot_area,
            number_of_bedrooms=self.test_bedrooms,
            number_of_bathrooms=self.test_bathrooms,
            year_built=self.test_year_built,
            status=self.test_status,
            price=Decimal("800000"),
            last_updated=self.test_last_updated,
            data_sources=self.test_data_sources
        )
        self.assertNotEqual(self.base_metadata, metadata2)

    def test_eq_price_none_comparison(self) -> None:
        """Test price comparison when one or both prices are None."""
        # Both None
        metadata_no_price1 = IPropertyMetadata(
            address=self.test_address,
            area=self.test_area,
            property_type=self.test_property_type,
            lot_area=self.test_lot_area,
            number_of_bedrooms=self.test_bedrooms,
            number_of_bathrooms=self.test_bathrooms,
            year_built=self.test_year_built,
            status=self.test_status,
            price=None,
            last_updated=self.test_last_updated,
            data_sources=self.test_data_sources
        )
        metadata_no_price2 = IPropertyMetadata(
            address=self.test_address,
            area=self.test_area,
            property_type=self.test_property_type,
            lot_area=self.test_lot_area,
            number_of_bedrooms=self.test_bedrooms,
            number_of_bathrooms=self.test_bathrooms,
            year_built=self.test_year_built,
            status=self.test_status,
            price=None,
            last_updated=self.test_last_updated,
            data_sources=self.test_data_sources
        )
        self.assertEqual(metadata_no_price1, metadata_no_price2)

        # One None, one with price
        self.assertNotEqual(metadata_no_price1, self.base_metadata)

    def test_eq_different_last_updated(self) -> None:
        """Test that metadata with different last_updated timestamps are not equal."""
        different_timestamp = datetime(2024, 1, 16, 10, 30, 0, tzinfo=timezone.utc)
        metadata2 = IPropertyMetadata(
            address=self.test_address,
            area=self.test_area,
            property_type=self.test_property_type,
            lot_area=self.test_lot_area,
            number_of_bedrooms=self.test_bedrooms,
            number_of_bathrooms=self.test_bathrooms,
            year_built=self.test_year_built,
            status=self.test_status,
            price=self.test_price,
            last_updated=different_timestamp,
            data_sources=self.test_data_sources
        )
        self.assertNotEqual(self.base_metadata, metadata2)

    def test_eq_with_non_metadata_object(self) -> None:
        """Test that comparison with non-IPropertyMetadata objects returns False."""
        self.assertNotEqual(self.base_metadata, "not a metadata object")
        self.assertNotEqual(self.base_metadata, 123)
        self.assertNotEqual(self.base_metadata, None)

    def test_is_equal_exclude_last_updated_false(self) -> None:
        """Test that is_equal with exclude_last_updated=False behaves same as __eq__."""
        metadata2 = IPropertyMetadata(
            address=self.test_address,
            area=self.test_area,
            property_type=self.test_property_type,
            lot_area=self.test_lot_area,
            number_of_bedrooms=self.test_bedrooms,
            number_of_bathrooms=self.test_bathrooms,
            year_built=self.test_year_built,
            status=self.test_status,
            price=self.test_price,
            last_updated=self.test_last_updated,
            data_sources=self.test_data_sources
        )

        # Should be equal
        self.assertTrue(self.base_metadata.is_equal(metadata2, exclude_last_updated=False))
        self.assertEqual(self.base_metadata, metadata2)

        # Test with different last_updated
        different_timestamp = datetime(2024, 1, 16, 10, 30, 0, tzinfo=timezone.utc)
        metadata3 = IPropertyMetadata(
            address=self.test_address,
            area=self.test_area,
            property_type=self.test_property_type,
            lot_area=self.test_lot_area,
            number_of_bedrooms=self.test_bedrooms,
            number_of_bathrooms=self.test_bathrooms,
            year_built=self.test_year_built,
            status=self.test_status,
            price=self.test_price,
            last_updated=different_timestamp,
            data_sources=self.test_data_sources
        )

        # Should not be equal
        self.assertFalse(self.base_metadata.is_equal(metadata3, exclude_last_updated=False))
        self.assertNotEqual(self.base_metadata, metadata3)

    def test_is_equal_exclude_last_updated_true(self) -> None:
        """Test that is_equal with exclude_last_updated=True ignores last_updated field."""
        different_timestamp = datetime(2024, 1, 16, 10, 30, 0, tzinfo=timezone.utc)
        metadata2 = IPropertyMetadata(
            address=self.test_address,
            area=self.test_area,
            property_type=self.test_property_type,
            lot_area=self.test_lot_area,
            number_of_bedrooms=self.test_bedrooms,
            number_of_bathrooms=self.test_bathrooms,
            year_built=self.test_year_built,
            status=self.test_status,
            price=self.test_price,
            last_updated=different_timestamp,  # Different timestamp
            data_sources=self.test_data_sources
        )

        # Should be equal when excluding last_updated
        self.assertTrue(self.base_metadata.is_equal(metadata2, exclude_last_updated=True))

        # But should not be equal with __eq__ (which includes last_updated)
        self.assertNotEqual(self.base_metadata, metadata2)

    def test_is_equal_exclude_last_updated_true_different_other_fields(self) -> None:
        """Test that is_equal with exclude_last_updated=True still checks other fields."""
        different_timestamp = datetime(2024, 1, 16, 10, 30, 0, tzinfo=timezone.utc)
        metadata2 = IPropertyMetadata(
            address=self.test_address,
            area=self.test_area,
            property_type=self.test_property_type,
            lot_area=self.test_lot_area,
            number_of_bedrooms=self.test_bedrooms,
            number_of_bathrooms=self.test_bathrooms,
            year_built=self.test_year_built,
            status=PropertyStatus.Sold,  # Different status
            price=self.test_price,
            last_updated=different_timestamp,
            data_sources=self.test_data_sources
        )

        # Should not be equal even when excluding last_updated due to different status
        self.assertFalse(self.base_metadata.is_equal(metadata2, exclude_last_updated=True))

    def test_is_equal_exclude_last_updated_true_different_price(self) -> None:
        """Test that is_equal with exclude_last_updated=True still checks price."""
        different_timestamp = datetime(2024, 1, 16, 10, 30, 0, tzinfo=timezone.utc)
        metadata2 = IPropertyMetadata(
            address=self.test_address,
            area=self.test_area,
            property_type=self.test_property_type,
            lot_area=self.test_lot_area,
            number_of_bedrooms=self.test_bedrooms,
            number_of_bathrooms=self.test_bathrooms,
            year_built=self.test_year_built,
            status=self.test_status,
            price=Decimal("800000"),  # Different price
            last_updated=different_timestamp,
            data_sources=self.test_data_sources
        )

        # Should not be equal even when excluding last_updated due to different price
        self.assertFalse(self.base_metadata.is_equal(metadata2, exclude_last_updated=True))

class Test_IPropertyHistoryEvent(unittest.TestCase):
    def test_equality_identical_events(self) -> None:
        """Test that identical events are equal."""
        event1 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source="Redfin",
            source_id="12345",
            price=Decimal(1000000)
        )
        event2 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source="Redfin",
            source_id="12345",
            price=Decimal(1000000)
        )
        self.assertEqual(event1, event2)

    def test_equality_different_ids(self) -> None:
        """Test that events with different IDs but same other properties are equal."""
        event1 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source="Redfin",
            source_id="12345",
            price=Decimal(1000000)
        )
        event2 = IPropertyHistoryEvent(
            "event2",  # Different ID
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source="Redfin",
            source_id="12345",
            price=Decimal(1000000)
        )
        self.assertEqual(event1, event2)

    def test_equality_different_descriptions(self) -> None:
        """Test that events with different descriptions but same other properties are equal."""
        event1 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source="Redfin",
            source_id="12345",
            price=Decimal(1000000)
        )
        event2 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed for sale",  # Different description
            source="Redfin",
            source_id="12345",
            price=Decimal(1000000)
        )
        self.assertEqual(event1, event2)

    def test_inequality_different_datetime(self) -> None:
        """Test that events with different datetime are not equal."""
        event1 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source="Redfin",
            source_id="12345",
            price=Decimal(1000000)
        )
        event2 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 2),  # Different datetime
            PropertyHistoryEventType.Listed,
            "Listed",
            source="Redfin",
            source_id="12345",
            price=Decimal(1000000)
        )
        self.assertNotEqual(event1, event2)

    def test_inequality_different_event_type(self) -> None:
        """Test that events with different event types are not equal."""
        event1 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source="Redfin",
            source_id="12345",
            price=Decimal(1000000)
        )
        event2 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Sold,  # Different event type
            "Listed",
            source="Redfin",
            source_id="12345",
            price=Decimal(1000000)
        )
        self.assertNotEqual(event1, event2)

    def test_inequality_different_price(self) -> None:
        """Test that events with different prices are not equal."""
        event1 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source="Redfin",
            source_id="12345",
            price=Decimal(1000000)
        )
        event2 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source="Redfin",
            source_id="12345",
            price=Decimal(950000)  # Different price
        )
        self.assertNotEqual(event1, event2)

    def test_inequality_different_source(self) -> None:
        """Test that events with different sources are not equal."""
        event1 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source="Redfin",
            source_id="12345",
            price=Decimal(1000000)
        )
        event2 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source="Zillow",  # Different source
            source_id="12345",
            price=Decimal(1000000)
        )
        self.assertNotEqual(event1, event2)

    def test_inequality_different_source_id(self) -> None:
        """Test that events with different source IDs are not equal."""
        event1 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source="Redfin",
            source_id="12345",
            price=Decimal(1000000)
        )
        event2 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source="Redfin",
            source_id="67890",  # Different source ID
            price=Decimal(1000000)
        )
        self.assertNotEqual(event1, event2)

    def test_equality_none_values(self) -> None:
        """Test equality with None values for optional fields."""
        event1 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source=None,
            source_id=None,
            price=None
        )
        event2 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source=None,
            source_id=None,
            price=None
        )
        self.assertEqual(event1, event2)

    def test_equality_mixed_none_values(self) -> None:
        """Test equality with some None values and some set values."""
        event1 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source="Redfin",
            source_id=None,
            price=Decimal(1000000)
        )
        event2 = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed",
            source="Redfin",
            source_id=None,
            price=Decimal(1000000)
        )
        self.assertEqual(event1, event2)

    def test_equality_with_other_types(self) -> None:
        """Test that equality returns NotImplemented for non-IPropertyHistoryEvent objects."""
        event = IPropertyHistoryEvent(
            "event1",
            datetime(2022, 1, 1),
            PropertyHistoryEventType.Listed,
            "Listed"
        )
        # Should return NotImplemented, which Python handles as False
        self.assertFalse(event == "not an event")

class Test_IPropertyHistory(unittest.TestCase):

    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        super().setUp()
        self.test_last_updated = datetime.now(timezone.utc)

    def test_history_events(self) -> None:
        address = IPropertyAddress("1838 Market St,Kirkland, WA 98033")
        events = [
            IPropertyHistoryEvent("event1", datetime(2022, 1, 1), PropertyHistoryEventType.Listed, "Listed", price=Decimal(1000000), source="Redfin", source_id="12345"),
            IPropertyHistoryEvent("event2", datetime(2022, 3, 1), PropertyHistoryEventType.Sold, "Sold", price=Decimal(940000), source="Redfin", source_id="12346"),
        ]
        history = IPropertyHistory(address, events, self.test_last_updated)
        self.assertEqual(len(history._history), 2)
        history.addEvent(IPropertyHistoryEvent("event3", datetime(2022, 2, 1), PropertyHistoryEventType.PriceChange, "Price dropped", price=Decimal(950000), source="Redfin", source_id="12347"))
        self.assertEqual(len(history._history), 3)

        self.assertEqual(history._history[2]._event_type, PropertyHistoryEventType.Sold)

    def test_basic_merge(self) -> None:
        """Test merging two histories with different events that don't overlap."""
        address = IPropertyAddress("1838 Market St,Kirkland, WA 98033")

        # Create first history
        events1 = [
            IPropertyHistoryEvent("event1", datetime(2022, 1, 1), PropertyHistoryEventType.Listed, "Listed", price=Decimal(1000000), source="Redfin", source_id="12345"),
            IPropertyHistoryEvent("event2", datetime(2022, 2, 1), PropertyHistoryEventType.PriceChange, "Price dropped", price=Decimal(950000), source="Redfin", source_id="12346"),
        ]
        history1 = IPropertyHistory(address, events1, self.test_last_updated)

        # Create second history
        events2 = [
            IPropertyHistoryEvent("event3", datetime(2022, 3, 1), PropertyHistoryEventType.Sold, "Sold", price=Decimal(940000), source="Redfin", source_id="12347"),
        ]
        history2 = IPropertyHistory(address, events2, self.test_last_updated)

        # Merge histories
        merged = IPropertyHistory.merge_history(history1, history2)

        # Verify results
        self.assertEqual(len(merged.history), 3)
        self.assertEqual(merged.address, address)

        # Verify chronological ordering
        self.assertEqual(merged.history[0].event_type, PropertyHistoryEventType.Listed)
        self.assertEqual(merged.history[1].event_type, PropertyHistoryEventType.PriceChange)
        self.assertEqual(merged.history[2].event_type, PropertyHistoryEventType.Sold)

    def test_duplicate_event_removal(self) -> None:
        """Test merging histories with overlapping events - duplicates should be removed."""
        address = IPropertyAddress("1838 Market St,Kirkland, WA 98033")

        # Create identical events
        event1 = IPropertyHistoryEvent("event1", datetime(2022, 1, 1), PropertyHistoryEventType.Listed, "Listed", price=Decimal(1000000), source="Redfin", source_id="12345")
        event2 = IPropertyHistoryEvent("event1", datetime(2022, 1, 1), PropertyHistoryEventType.Listed, "Listed", price=Decimal(1000000), source="Redfin", source_id="12345")

        history1 = IPropertyHistory(address, [event1], self.test_last_updated)
        history2 = IPropertyHistory(address, [event2], self.test_last_updated)

        # Merge histories
        merged = IPropertyHistory.merge_history(history1, history2)

        # Verify duplicate was removed
        self.assertEqual(len(merged.history), 1)
        self.assertEqual(merged.history[0].event_type, PropertyHistoryEventType.Listed)
        self.assertEqual(merged.history[0].price, Decimal(1000000))

    def test_identical_history_merge(self) -> None:
        """Test merging two identical histories - result should be exactly the same."""
        address = IPropertyAddress("1838 Market St,Kirkland, WA 98033")

        # Create identical events
        events = [
            IPropertyHistoryEvent("event1", datetime(2022, 1, 1), PropertyHistoryEventType.Listed, "Listed", price=Decimal(1000000), source="Redfin", source_id="12345"),
            IPropertyHistoryEvent("event2", datetime(2022, 3, 1), PropertyHistoryEventType.Sold, "Sold", price=Decimal(940000), source="Redfin", source_id="12346"),
        ]

        history1 = IPropertyHistory(address, events, self.test_last_updated)
        history2 = IPropertyHistory(address, events, self.test_last_updated)

        # Merge histories
        merged = IPropertyHistory.merge_history(history1, history2)

        # Verify result is identical to original
        self.assertEqual(len(merged.history), 2)
        self.assertEqual(merged.address, address)
        self.assertEqual(merged.history[0].event_type, PropertyHistoryEventType.Listed)
        self.assertEqual(merged.history[1].event_type, PropertyHistoryEventType.Sold)
        self.assertEqual(merged.history[0].price, Decimal(1000000))
        self.assertEqual(merged.history[1].price, 940000)

    def test_different_address_validation(self) -> None:
        """Test that merging histories with different addresses raises ValueError."""
        address1 = IPropertyAddress("1838 Market St,Kirkland, WA 98033")
        address2 = IPropertyAddress("456 Oak Ave,Redmond, WA 98052")

        history1 = IPropertyHistory(address1, [], self.test_last_updated)
        history2 = IPropertyHistory(address2, [], self.test_last_updated)

        # Verify ValueError is raised
        with self.assertRaises(ValueError) as context:
            IPropertyHistory.merge_history(history1, history2)

        self.assertIn("Cannot merge histories with different addresses", str(context.exception))

    def test_last_updated_timestamp(self) -> None:
        """Test that merged history uses the latest last_updated timestamp."""
        address = IPropertyAddress("1838 Market St,Kirkland, WA 98033")

        # Create histories with different timestamps
        timestamp1 = datetime(2022, 1, 1, 10, 0, 0)
        timestamp2 = datetime(2022, 1, 1, 15, 0, 0)

        history1 = IPropertyHistory(address, [], last_updated=timestamp1)
        history2 = IPropertyHistory(address, [], last_updated=timestamp2)

        # Merge histories
        merged = IPropertyHistory.merge_history(history1, history2)

        # Verify latest timestamp is used
        self.assertEqual(merged.last_updated, timestamp2)

    def test_complex_merge_with_multiple_duplicates(self) -> None:
        """Test merging histories with multiple overlapping events and different event types."""
        address = IPropertyAddress("1838 Market St,Kirkland, WA 98033")

        # Create first history
        events1 = [
            IPropertyHistoryEvent("event1", datetime(2022, 1, 1), PropertyHistoryEventType.Listed, "Listed", price=Decimal(1000000), source="Redfin", source_id="12345"),
            IPropertyHistoryEvent("event2", datetime(2022, 2, 1), PropertyHistoryEventType.PriceChange, "Price dropped", price=Decimal(950000), source="Redfin", source_id="12346"),
            IPropertyHistoryEvent("event3", datetime(2022, 3, 1), PropertyHistoryEventType.Sold, "Sold", price=Decimal(940000), source="Redfin", source_id="12347"),
        ]
        history1 = IPropertyHistory(address, events1, self.test_last_updated)

        # Create second history with some duplicates and new events
        events2 = [
            IPropertyHistoryEvent("event1", datetime(2022, 1, 1), PropertyHistoryEventType.Listed, "Listed", price=Decimal(1000000), source="Redfin", source_id="12345"),  # duplicate
            IPropertyHistoryEvent("event4", datetime(2022, 2, 15), PropertyHistoryEventType.PriceChange, "Price increased", price=Decimal(960000), source="Redfin", source_id="12348"),
            IPropertyHistoryEvent("event5", datetime(2022, 2, 20), PropertyHistoryEventType.Pending, "Pending", price=Decimal(940000), source="Redfin", source_id="12349"),
        ]
        history2 = IPropertyHistory(address, events2, self.test_last_updated)

        # Merge histories
        merged = IPropertyHistory.merge_history(history1, history2)

        # Verify results
        self.assertEqual(len(merged.history), 5)  # 5 unique events
        self.assertEqual(merged.address, address)

        # Verify chronological ordering
        expected_order = [
            PropertyHistoryEventType.Listed,      # 2022-01-01
            PropertyHistoryEventType.PriceChange, # 2022-02-01
            PropertyHistoryEventType.PriceChange, # 2022-02-15
            PropertyHistoryEventType.Pending,     # 2022-02-20
            PropertyHistoryEventType.Sold,        # 2022-03-01
        ]

        for i, expected_type in enumerate(expected_order):
            self.assertEqual(merged.history[i].event_type, expected_type)

    def test_empty_history_merge(self) -> None:
        """Test merging with one or both histories being empty."""
        address = IPropertyAddress("1838 Market St,Kirkland, WA 98033")

        # Test merging empty with non-empty
        empty_history = IPropertyHistory(address, [], self.test_last_updated)
        non_empty_history = IPropertyHistory(
            address,
            [
                IPropertyHistoryEvent("event1", datetime(2022, 1, 1), PropertyHistoryEventType.Listed, "Listed", price=Decimal(1000000), source="Redfin", source_id="12345")
            ],
            self.test_last_updated,
            )

        # Merge empty with non-empty
        merged1 = IPropertyHistory.merge_history(empty_history, non_empty_history)
        self.assertEqual(len(merged1.history), 1)
        self.assertEqual(merged1.history[0].event_type, PropertyHistoryEventType.Listed)

        # Merge non-empty with empty
        merged2 = IPropertyHistory.merge_history(non_empty_history, empty_history)
        self.assertEqual(len(merged2.history), 1)
        self.assertEqual(merged2.history[0].event_type, PropertyHistoryEventType.Listed)

        # Merge two empty histories
        merged3 = IPropertyHistory.merge_history(empty_history, empty_history)
        self.assertEqual(len(merged3.history), 0)

if __name__ == '__main__':
    unittest.main()