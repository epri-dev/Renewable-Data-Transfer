from collections.abc import Iterable
from datetime import datetime
from typing import Union

from PIconnect import _time, PIConsts
from PIconnect.AFSDK import AF  # , AF_SDK_VERSION
from PIconnect.PIConsts import (
    CalculationBasis,
    ExpressionSampleType,
    SummaryType,
    TimestampCalculation,
    RetrievalMode,
)
from PIconnect.PIData import PISeriesContainer, PISeries
from PIconnect.PIPoint import PIPoint
from PIconnect._typing.Generic import TimeSpan

from System import String, Array  # From clr via PIconnect
from System.Collections.Generic import IEnumerable, List, IList
from pandas import (
    Timestamp,
    to_datetime,
    concat,
)

POINT_TYPES = {
    AF.PI.PIPointType.Int16: int,
    AF.PI.PIPointType.Int32: int,
    AF.PI.PIPointType.Float16: float,
    AF.PI.PIPointType.Float32: float,
    AF.PI.PIPointType.Float64: float,
    AF.PI.PIPointType.Digital: str,
    AF.PI.PIPointType.Timestamp: to_datetime,
    AF.PI.PIPointType.String: str,
}


class BoundaryType:
    """PI Boundary Type.  Can be abstracted directly instead of using hard-coded type labels.
    """
    Inside = AF.Data.AFBoundaryType.Inside
    Outside = AF.Data.AFBoundaryType.Outside
    Interpolated = AF.Data.AFBoundaryType.Interpolated


def _extract_afvalue(afvalue: AF.Asset.AFValue, dtype: type = None):
    """
    Extracts query results

    Query results in the `AFValues` object are extracted into numpy array(s)
    and then converted into a pandas series object.

    Parameters
    ----------
    afvalue : AFValues or IDictionary<AFSummaryTypes, AFValues>
        AFValues collection or A dictionary of AFValues, indexed by the specific
        AFSummaryTypes requested from the PI Archive Server.

    Returns
    -------
    Tuple
        Collection of string representation of time and associated value

    Examples
    --------
    >>> _extract_afvalue(afvalue)
    (01/01/2019 00:00:00.000000, 120.4)
    """
    if dtype is None:
        dtype = POINT_TYPES.get(afvalue.PIPoint.PointType)

    if afvalue.IsGood:
        try:
            val = dtype(afvalue.Value)
        except TypeError:
            val = float('NaN')
    else:
        if dtype == float or dtype == int:
            val = float('NaN')
        else:
            val = ''

    return val


def _extract_aftime(afvalue: AF.Asset.AFValue):
    """Extracts timestamp from afvalue"""
    return afvalue.Timestamp.UtcTime.ToString('MM/dd/yyyy HH:mm:ss.ffffff')


class PIPointList(PISeriesContainer):
    """
    Container for AF PIPointList object containing PIPoint objects and their attributes.

    PIPointList object initialization

    Configures PIPointList creating and PIPagingConfiguration options

    Parameters
    ----------
    pi_point_list : list
        List of PIPoint objects
    page_type : str, default 'TagCount'
        Used to determine how partial results from list data access calls should be grouped while
        being returned to the client. Choosing the appropriate page type will improve performance.

        Options:
        - TagCount: Paged by tag count, with a maximum tags per page specified in the PageSize property.
        - EventCount: Paged from the PI Data Archive by event count.

    page_size : int, default 10
        Size of the pages that will be returned depending on the PageType.
    max_retries : int, default 5
        Maximum number of times to retry when encountering an error from PIServer before exceeding the
        maximum number of concurrent bulk queries.
    operation_timeout : TimeSpan, default None
        Overrides the operation timeout set on the PIServer for the duration of the data access call.
        `operation_timeout` is the maximum amount of time that can elapse on the PIServer while fetching
        each page.
    keep_alive_timeout : TimeSpan, default None
        Maximum amount of time allowed to elapse between calls to get the next page of results.
    bulk_payload_pct : TimeSpan, default None
        Percentage of the entire RPC that must be completed to cause a page to be proactively returned
        to prevent the operation timeout from expiring.

    Returns
    -------
    PIPointList
        Return type selected by value of `return_point_list`

    """
    __allowed_types = (AF.PI.PIPoint, PIPoint)

    _page_types = dict(
        TagCount=AF.PI.PIPageType.TagCount,
        EventCount=AF.PI.PIPageType.EventCount,
    )

    def __init__(
            self,
            pi_point_list=None,
            page_type='TagCount',
            page_size=10,
            max_retries=5,
            operation_timeout=None,
            keep_alive_timeout=None,
            bulk_payload_pct=None
    ):
        self.idx = 0

        super(PIPointList, self).__init__()
        self.__attributes_loaded = False
        self.__attributes = {}
        self.__servers = None
        self.__page_config = None

        # Test if pi_point_list is a non-empty list
        if pi_point_list and isinstance(pi_point_list, AF.PI.PIPointList):
            self.pi_point_list = pi_point_list
        else:
            self.pi_point_list = AF.PI.PIPointList()

        self.page_type = page_type
        self.page_size = page_size
        self.max_retries = max_retries
        self.operation_timeout = operation_timeout
        self.keep_alive_timeout = keep_alive_timeout
        self.bulk_payload_pct = bulk_payload_pct

    def __repr__(self):
        return f'{self.__class__.__name__}(Contains {self.count} PIPoints)'

    def __iter__(self):
        return self

    def __next__(self):
        if self.idx >= self.pi_point_list.Count:
            self.idx = 0
            raise StopIteration
        else:
            self.idx += 1
            return self.pi_point_list[self.idx - 1]

    next = __next__

    def __getitem__(self, item):
        if isinstance(item, Iterable):
            if all(i is int for i in item):
                indices = item
            elif all(i is str for i in item):
                tags = [p.GetPath() for p in self.pi_point_list]
                indices = [tags.index(v) for v in item]
            return PIPointList(pi_point_list=[self.pi_point_list[i] for i in indices])
            # tags = [self.tags[i] for i in indices]
            # units = [self.units[i] for i in indices]
            # ppl = PI.AFSDK.AF.PI.PIPointList(Array[PI.AFSDK.AF.PI.PIPoint]([self.pi_point_list[i] for i in indices]))
            # return PIDataModel(self.name, item, item, is_subset=True, pi_point_list=ppl)

    @property
    def page_type(self):
        return self.__page_type

    @page_type.setter
    def page_type(self, page_type):
        new_page_type = self._page_types.get(page_type)
        if new_page_type is None:
            raise TypeError('page_type attribute must be one of ' + ', '.join(
                '"%s"' % type(x) for x in self._page_types))
        else:
            self.__page_type = new_page_type

    @property
    def page_size(self):
        return self.__page_size

    @page_size.setter
    def page_size(self, page_size):
        if isinstance(page_size, int):
            if page_size > 0:
                self.__page_size = page_size
            else:
                raise ValueError('page_size attribute must be greater than zero')
        else:
            raise TypeError('page_size attribute must be of type int')

    @property
    def max_retries(self):
        if self.__max_retries:
            return self.__max_retries
        else:
            return None

    @max_retries.setter
    def max_retries(self, max_retries):
        """Number of retries before stopping query"""
        if max_retries:
            if isinstance(max_retries, int):
                if max_retries > 0:
                    self.__max_retries = max_retries
                else:
                    raise ValueError('max_retries attribute must be greater than zero')
            else:
                raise TypeError('max_retries attribute must be of type int')
        else:
            self.__max_retries = None

    @property
    def operation_timeout(self):
        """
        Timeout, in seconds, for the duration of the data access call.  The max
        amount of time that can elapse on the PIServer while fetching each page.
        """
        return self.__operation_timeout

    @operation_timeout.setter
    def operation_timeout(self, operation_timeout):
        if operation_timeout is not None:
            if isinstance(operation_timeout, int):
                if operation_timeout > 0:
                    self.__operation_timeout = TimeSpan(0, 0, operation_timeout)
                else:
                    raise ValueError("operation_timeout attribute must be greater than zero")
            else:
                raise TypeError('operation_timeout attribute must be of type int')
        else:
            self.__operation_timeout = None

    @property
    def keep_alive_timeout(self):
        """Timeout, in seconds, to keep the server connection alive between data calls."""
        return self.__keep_alive_timeout

    @keep_alive_timeout.setter
    def keep_alive_timeout(self, keep_alive_timeout):
        if keep_alive_timeout:
            if isinstance(keep_alive_timeout, int):
                if keep_alive_timeout > 0:
                    self.__keep_alive_timeout = \
                        TimeSpan(0, 0, keep_alive_timeout)
                else:
                    raise ValueError('keep_alive_timeout attribute must be greater than zero')
            else:
                raise TypeError('keep_alive_timeout attribute must be of type int')
        else:
            self.__keep_alive_timeout = None

    @property
    def bulk_payload_pct(self):
        return self.__bulk_payload_pct

    @bulk_payload_pct.setter
    def bulk_payload_pct(self, bulk_payload_pct):
        if bulk_payload_pct:
            if isinstance(bulk_payload_pct, int):
                if bulk_payload_pct > 0:
                    self.__bulk_payload_pct = 0
                else:
                    raise ValueError('bulk_payload_pct attribute must be greater than zero')
            else:
                raise TypeError('bulk_payload_pct attribute must be of type int')
        else:
            self.__bulk_payload_pct = None

    @property
    def page_config(self):
        """Return the page configuration property for the page configuration option."""
        # self.__page_config
        if self.max_retries is not None:
            self.__page_config = AF.PI.PIPagingConfiguration(
                self.page_type,
                self.page_size,
                self.max_retries,
                self.operation_timeout,
                self.keep_alive_timeout,
                self.bulk_payload_pct,
            )
        else:
            self.__page_config = AF.PI.PIPagingConfiguration(
                self.page_type,
                self.page_size,
                self.operation_timeout,
                self.keep_alive_timeout,
                self.bulk_payload_pct,
            )
        return self.__page_config

    @property
    def servers(self):
        return [server.Name for server in self.pi_point_list.GetServers()]

    @property
    def count(self):
        """Number of PIPoint object references within PIPointList."""
        return self.pi_point_list.Count

    def add(self, pi_point: Union[AF.PI.PIPoint, PIPoint]):
        """
        Adds provided pi_point object to the PIPointList, and the list of PIPoint names.

        Parameters
        ----------
        pi_point : AF.PI.PIPoint or PIPoint
            AF SDK PIPoint or PIconnect PIPoint object to be added to the current PIPointList.
        """
        if isinstance(pi_point, AF.PI.PIPoint):
            self.pi_point_list.Add(pi_point)
        elif isinstance(pi_point, PIPoint):
            self.pi_point_list.Add(pi_point.pi_point)
        else:
            raise TypeError(
                'pi_point_list attribute elements must be one of ' +
                ', '.join('"%s"' % type(x) for x in self.__allowed_types)
            )

    def add_range(self, points: IEnumerable[AF.PI.PIPoint] | IList[AF.PI.PIPoint] | list):
        """
        Adds provided pi_point object to the PIPointList, and the list of PIPoint names.

        Parameters
        ----------
        points : System.Collections.Generic.IEnumerable[PIPoint], System.Collections.Generic.Ilist[PIPoint], list
            AF SDK PIPoint objects to be added to the current PIPointList.

        See Also
        --------
        get_enumerable : Get IList for PIPointList so it can be added to another PIPointList

        Examples
        --------
        Extract the enumerable List

        >>> second_server = PIServer('second')
        >>> second_pipointlist = second_server.search('sunu*')
        >>> enumerable = second_pipointlist.get_enumerable

        Add the enumerable list of PIPoints to another PIPointList

        >>> default_server = PIServer()
        >>> pipointlist = default_server.search('sunu*')
        >>> pipointlist = pipointlist.add(enumerable)

        Add one PIPointList to another PIPointList

        >>> default_server = PIServer()
        >>> pipointlist = default_server.search('sunu*')
        >>> second_server = PIServer('second')
        >>> second_pipointlist = second_server.search('sunu*')
        >>> pipointlist = pipointlist.add(second_pipointlist)
        """
        if isinstance(points, IEnumerable[AF.PI.PIPoint]):
            self.pi_point_list.AddRange(points)
        elif isinstance(points, IList[AF.PI.PIPoint]):
            self.pi_point_list.AddRange(IEnumerable[AF.PI.PIPoint](points))
        elif isinstance(points, type(self)):
            self.pi_point_list.AddRange(points.get_enumerable)
        elif isinstance(points, list):
            cList = List[AF.PI.PIPoint]()
            for point in points:
                cList.Add(point)
            self.pi_point_list.AddRange(IEnumerable[AF.PI.PIPoint](cList))

    def remove(self, pi_point):
        """
        Removes provided pi_point object to the PIPointList, and the list of PIPoint names.

        Parameters
        ----------
        pi_point : AF.PI.PIPoint
            AF SDK PIPoint object to be added to the current PIPointList.
        """
        self.pi_point_list.Remove(pi_point)

    @property
    def attributes(self):
        """
        Return a dictionary of the raw attributes of the PIPoint.

        .. deprecated:: 1.3.0
                Replaced by ``get_attributes``.

        See Also
        --------
        get_attributes : Get attributes of the PIPoint(s)
        """
        # self.__load_attributes()
        return self.__get_pi_point_attributes()

    @property
    def get_point_list_strings(self):
        return self.points_list

    @property
    def points_list(self):
        """
        Names of PIPoints expressed as a list of strings

        Returns
        -------
        List
            Point names for all PIPoints in the PIPointList

        Examples
        --------
        >>> with PIServer() as server:
        >>>     points = server.search("sinu*")
        >>>     points.points_list
        ['SINUSOID', 'sinusoid1', 'SINUSOIDU', 'sinusoid_ghj']
        """
        return [self.get_point_name(point) for point in self]

    @property
    def point_ids(self):
        """
        Return the point IDs for each **PIPoint** in the ``PIPointList``

        Returns
        -------
        Dict
            Dictionary of PIPoint names and PIPoint PointIDs

        Examples
        --------
        >>> with PIServer() as server:
        >>>     points = server.search("sinu*")
        >>>     points.point_ids
        {'SINUSOID': 44494, 'sinusoid1': 118578, 'SINUSOIDU': 44501, 'sinusoid_ghj': 129779}
        """
        return {self.get_point_name(point): point.ID for point in self}

    def point_attributes(self, attributes: list = None):
        """
        Get attributes of the PIPoints in the PIPointList

        .. deprecated:: 1.3.0
                Replaced by ``get_attributes``.

        Parameters
        ----------
        attributes : list, default None
            List of attributes to retrieve from the PIPointList

        See Also
        --------
        get_attributes : Get attributes of the PIPoint(s)

        Returns
        -------
        Dict
            Dictionary of requested attributes by each PIPoint Name
        """
        return self.get_attributes(attributes)

    # @Appender(_doc_get_attributes)
    def get_attributes(self, attributes: list = None):
        self.__load_attributes()
        if attributes is None:
            attributes = []
        return {
            self.get_point_name(point): {
                att.Key.lower(): self._convert_attribute(att.Value) for att in point.GetAttributes(attributes)
            }
            for point in self.pi_point_list
        }

    @property
    def descriptors(self):
        """
        Returns descriptors for each PIPoint in the ``PIPointList``

        .. deprecated:: 1.3.0
                Replaced by ``get_attributes``.

        See Also
        --------
        get_attributes : Get attributes of the PIPoint(s)
        """
        return self.__get_pi_point_attribute('descriptor')
        # return self.__get_pi_point_attributes(AF.PI.PICommonPointAttributes.Descriptor)

    @property
    def extended_descriptors(self):
        """
        Returns Extended Description for each PIPoint in the ``PIPointList``

        .. deprecated:: 1.3.0
                Replaced by ``get_attributes``.

        See Also
        --------
        get_attributes : Get attributes of the PIPoint(s)
        """
        return self.__get_pi_point_attribute('exdesc')

    # @property
    # def current_values(self):
    #     results_dict = {}
    #     list_results = self.pi_point_list.CurrentValue()
    #     for afvalue in list_results:
    #         ts, val = _extract_aftime(afvalue), _extract_afvalue(afvalue)
    #         results_dict[afvalue.PIPoint.Name] = (Timestamp(ts, tz='utc'), val)
    #
    #     return results_dict

    @property
    def get_enumerable(self):
        """
        Get IList for PIPointList so it can be added to another PIPointList

        Allows for more tailored PIPoint filtering and searching across multipler PIServers

        Returns
        -------
        IList<AF.PI.PIPoint>
            Enumerable list of PIPoints

        See Also
        --------
        add_range : Adds provided pi_point object to the PIPointList

        Examples
        --------
        Extracting the enumerable List

        >>> default_server = PIServer()
        >>> pipointlist = default_server.search(['sunu*'])
        >>> enumberable = pipointlist.get_enumerable
        """
        com_list = List[AF.PI.PIPoint]()
        for pipoint in self:
            com_list.Add(pipoint)
        return IList[AF.PI.PIPoint](com_list)

    def _current_value(self):
        """
        Get current value from snapshot table

        Returns
        -------
        AF.AFListResults<PIPoint, AFValue>

        See Also
        --------
        _plot_values : Values at regular intervals (pixels) for plotting
        _summaries : Data aggregations
        _filtered_summaries : Filtered data aggregations
        _interpolated_values : Values at regular intervals defined by TimeSpan
        _recorded_value : Last value recorded in the PI Archive
        """
        return self.pi_point_list.CurrentValue()

    def interpolated_values(
            self,
            start_time: datetime | Timestamp | str,
            end_time: datetime | Timestamp | str,
            # timespan: str = None,
            interval: str = None,
            filterexp: str = '',
            filter_expression: str = '',
            include_filtered_values: bool = False,
            include_flags: bool = False,
            operation_timeout: int = None,
            **kwargs
    ):
        self.operation_timeout = operation_timeout

        time_range = _time.to_af_time_range(start_time, end_time)
        _interval = AF.Time.AFTimeSpan.Parse(interval)
        _filter_expression = self._normalize_filter_expression(filter_expression)
        pivalues = self._interpolated_values(time_range, _interval, _filter_expression)

        serieses = []
        self.pi_point_list.LoadAttributes('engunits')
        for s in pivalues:
            timestamps: list[datetime.datetime] = []
            values: list = []
            for value in s:
                timestamps.append(_time.timestamp_to_index(value.Timestamp.UtcTime))
                values.append(value.Value)

            uom = s.PIPoint.GetAttribute('engunits')

            new_series = PISeries(  # type: ignore
                tag=s.PIPoint.GetPath(),
                timestamp=timestamps,
                value=values,
                uom=uom,
            )

            # values, flags = self._extract_ienumerable_afvalues(ienumerable_afvalues)

            # if not values:
            #     if include_flags:
            #         return None, None
            #     return None
            #
            # values = self.manage_tz(concat(values.values(), axis='columns'), **kwargs)
            # flags = self.manage_tz(concat(flags.values(), axis='columns'), **kwargs)
            #
            # results = values
            # if include_flags:
            #     results = results, flags

            serieses.append(new_series)

        return concat(serieses, axis=1)

    def _interpolated_values(self, time_range, interval, filter_expression):
        """
        Get values from the archive within the specified ``time_range`` and at the specified ``interval``.
        Returned results are provided at the specified ``interval`` by interpolating between the last recorded value
        before, and the first recorded value after the interval timestamp.

        Parameters
        ----------
        time_range : AF.Time.AFTimeRange
            Time range over which to query the PIPoint(s)
        interval : AF.Time.AFTimeSpan
            Interval at which to return values within the ''time_range''
        filter_expression : str
            OSISoft PI filter expression for returned recorded values
        include_filtered_values : bool
            Include the filtered values in the return data

        Returns
        -------
        AF.Asset.AFValues

        See Also
        --------
        _current_value : Current values in the PI Snapshot table
        _plot_values : Values at regular intervals (pixels) for plotting
        _summaries : Data aggregations
        _filtered_summaries : Filtered data aggregations
        _recorded_value : Last value recorded in the PI Archive
        _recorded_values : Values as they are recorded in the PI Archive
        """
        return self.pi_point_list.InterpolatedValues(time_range,
                                                     interval,
                                                     filter_expression,
                                                     False,
                                                     self.page_config)

    def _extract_ienumerable_afvalues(self, ienumerable_afvalues):
        results_dict = {}
        flags_dict = {}
        for afvalues in ienumerable_afvalues:
            if afvalues.Count == 0:
                continue

            results, flags = self._get_value_arrays(afvalues)
            results_dict[results.name] = results
            flags_dict[results.name] = flags

        return results_dict, flags_dict

    def plot_values(
            self,
            start_time: datetime | Timestamp | str,
            end_time: datetime | Timestamp | str,
            intervals: str | int,
            include_flags: bool = False,
            operation_timeout: int = None,
            **kwargs
    ):
        self.operation_timeout = operation_timeout

        ienumerable_afvalues = super().plot_values(
            start_time,
            end_time,
            intervals,
            **kwargs
        )

        values, flags = self._extract_ienumerable_afvalues(ienumerable_afvalues)

        if not values:
            if include_flags:
                return None, None
            return None

        results = values
        if include_flags:
            results = results, flags

        return results

    def _plot_values(self, time_range, intervals):
        """
        Get values from the archive within the specified ``time_range`` and at the specified ``interval``.
        Returned results are provided at the specified ``interval`` by interpolating between the last recorded value
        before, and the first recorded value after the interval timestamp.

        Parameters
        ----------
        time_range : AF.Time.AFTimeRange
            Time range over which to query the PIPoint(s)
        intervals : AF.Time.AFTimeSpan
            Number of intervals to return over the specified ''time_range''.

        Returns
        -------
        AF.Asset.AFValues

        See Also
        --------
        _current_value : Current values in the PI Snapshot table
        _summaries : Data aggregations
        _filtered_summaries : Filtered data aggregations
        _interpolated_values : Values at regular intervals defined by TimeSpan
        _recorded_value : Last value recorded in the PI Archive
        _recorded_values : Values as they are recorded in the PI Archive
        """
        return self.pi_point_list.PlotValues(time_range, intervals, self.page_config)

    def recorded_values(
            self,
            start_time: datetime | Timestamp | str,
            end_time: datetime | Timestamp | str,
            boundary_type: BoundaryType = BoundaryType.Inside,
            filter_expression: str = '',
            include_filtered_values: bool = False,
            include_flags: bool = False,
            operation_timeout: int = None,
            **kwargs
    ):
        self.operation_timeout = operation_timeout

        ienumerable_afvalues = super().recorded_values(
            start_time, end_time,
            boundary_type=boundary_type, filter_expression=filter_expression,
            include_filtered_values=include_filtered_values, **kwargs
        )

        values, flags = self._extract_ienumerable_afvalues(ienumerable_afvalues)

        if not values:
            if include_flags:
                return None, None
            return None

        results = values
        if include_flags:
            results = results, flags

        return results

    def _recorded_values(self, time_range, boundary_type, filter_expression, include_filtered_values):
        """
        Get values from the archive as they were recorded

        Parameters
        ----------
        time_range : AF.Time.AFTimeRange
            Time range over which to query the PIPoint(s)
        boundary_type : AF.Data.AFBoundaryType
            Value return option:

            - AF.Data.AFBoundaryType.Inside
            - AF.Data.AFBoundaryType.Outside
            - AF.Data.AFBoundaryType.Interpolated

        filter_expression : str
            OSISoft PI filter expression for returned recorded values
        include_filtered_values : bool
            Include the filtered values in the return data

        Returns
        -------
        AF.Asset.AFValues

        See Also
        --------
        _current_value : Current values in the PI Snapshot table
        _plot_values : Values at regular intervals (pixels) for plotting
        _summaries : Data aggregations
        _filtered_summaries : Filtered data aggregations
        _interpolated_values : Values at regular intervals defined by TimeSpan
        _recorded_value : Last value recorded in the PI Archive
        """
        return self.pi_point_list.RecordedValues(
            time_range,
            boundary_type,
            filter_expression,
            include_filtered_values,
            self.page_config
        )

    def recorded_value(
            self,
            record_time: datetime | Timestamp | str,
            retrieval_mode: RetrievalMode = RetrievalMode.AUTO
    ):
        afvalues = super().recorded_value(
            record_time, retrieval_mode=retrieval_mode
        )
        results_dict = {}
        for afvalue in afvalues:
            ts, val = _extract_aftime(afvalue), _extract_afvalue(afvalue)
            results_dict[self.get_point_name(afvalue.PIPoint)] = (Timestamp(ts, tz='UTC'), val)
        return results_dict

    def _recorded_value(self, af_time, retrieval_mode):
        """
        Get values from the archive as they were recorded

        Parameters
        ----------
        af_time : AF.Time.AFTime
            Time at which to query the PIPoint(s)
        retrieval_mode : AF.Data.AFBoundaryType
            Value return option:

            - AF.Data.AFBoundaryType.Inside
            - AF.Data.AFBoundaryType.Outside
            - AF.Data.AFBoundaryType.Interpolated

        Returns
        -------
        AF.Asset.AFValue

        See Also
        --------
        _current_value : Current values in the PI Snapshot table
        _plot_values : Values at regular intervals (pixels) for plotting
        _summaries : Data aggregations
        _filtered_summaries : Filtered data aggregations
        _interpolated_values : Values at regular intervals defined by TimeSpan
        _recorded_values : Values as they are recorded in the PI Archive
        """
        return self.pi_point_list.RecordedValue(af_time, retrieval_mode)

    def summaries(
            self,
            start_time: datetime | Timestamp | str,
            end_time: datetime | Timestamp | str,
            interval: str = None,
            summary_type: SummaryType = SummaryType.AVERAGE,
            calculation_basis: CalculationBasis = CalculationBasis.TIME_WEIGHTED,
            time_type: TimestampCalculation = TimestampCalculation.AUTO,
            filter_expr: str = None,
            filter_evaluation: ExpressionSampleType = ExpressionSampleType.EXPRESSION_RECORDED_VALUES,
            filter_interval: str = None,
            include_flags: bool = False,
            operation_timeout: int = None,
            **kwargs
    ):
        self.operation_timeout = operation_timeout

        _summary_types = AF.Data.AFSummaryTypes(summary_type)
        _calculation_basis = AF.Data.AFCalculationBasis(calculation_basis)
        _time_type = AF.Data.AFTimestampCalculation(time_type)

        time_range = _time.to_af_time_range(start_time, end_time)
        _interval = AF.Time.AFTimeSpan.Parse(interval)
        pivalues = self._summaries(time_range, _interval, _summary_types, _calculation_basis, _time_type)

        serieses = []
        # keys = []
        self.pi_point_list.LoadAttributes('engunits')
        timestamps = None
        for point in pivalues:
            for summary in point:
                values, ts, _ = summary.Value.GetValueArrays()
                if timestamps is None and not (len(values) == 1 and type(values[0]) is AF.PI.PIException):
                    timestamps = ts

                uom = summary.Value.PIPoint.GetAttribute('engunits')
                path = summary.Value.PIPoint.GetPath()
                key = PIConsts.SummaryType(int(summary.Key)).name
                serieses.append(PISeries((path, key), timestamp=None, value=values, uom=uom))

        final = concat(serieses, axis=1)

        timestamps = [_time.timestamp_to_index(t) for t in timestamps]
        final.index = timestamps

        return final

    def _summaries(self, time_range, interval, summary_types, calculation_basis, time_type):
        """
        Get calculation summary values from the archive within the specified ``time_range`` and at the specified
        ``interval``.  Returned results are provided at the specified ``interval`` by calculating the point summary
        over the ``interval``.

        Parameters
        ----------
        time_range : AF.Time.AFTimeRange
            Time range over which to query the PIPoint(s)
        interval : AF.Time.AFTimeSpan
            Interval at which to return values within the ''time_range''
        summary_types : AF.Data.AFSummaryTypes
            Type of summary (e.g. 'average', 'max, 'min) to provide
        calculation_basis : AF.Data.AFCalculationBasis
            Calculation basis (e.g. 'TimeWeighted', 'EventWeighted')
        time_type : AF.Data.AFTimestampCalculation
            Timestamp (start or end) to provide

        Returns
        -------
        AF.Asset.AFValues object containing the response values

        See Also
        --------
        _current_value : Current values in the PI Snapshot table
        _plot_values : Values at regular intervals (pixels) for plotting
        _filtered_summaries : Filtered data aggregations
        _interpolated_values : Values at regular intervals defined by TimeSpan
        _recorded_value : Last value recorded in the PI Archive
        _recorded_values : Values as they are recorded in the PI Archive
        """
        return self.pi_point_list.Summaries(time_range,
                                            interval,
                                            summary_types,
                                            calculation_basis,
                                            time_type,
                                            self.page_config)

    def _summary(self, time_range, summary_types, calculation_basis, time_type):
        """
        Get calculation summary values from the archive within the specified ``time_range`` and at the specified
        ``interval``.  Returned results are provided at the specified ``interval`` by calculating the point summary
        over the ``interval``.

        Parameters
        ----------
        time_range : AF.Time.AFTimeRange
            Time range over which to query the PIPoint(s)
        summary_types : AF.Data.AFSummaryTypes
            Type of summary (e.g. 'average', 'max, 'min) to provide
        calculation_basis : AF.Data.AFCalculationBasis
            Calculation basis (e.g. 'TimeWeighted', 'EventWeighted')
        time_type : AF.Data.AFTimestampCalculation
            Timestamp (start or end) to provide

        Returns
        -------
        AF.Asset.AFValues object containing the response values

        See Also
        --------
        _current_value : Current values in the PI Snapshot table
        _plot_values : Values at regular intervals (pixels) for plotting
        _filtered_summaries : Filtered data aggregations
        _interpolated_values : Values at regular intervals defined by TimeSpan
        _recorded_value : Last value recorded in the PI Archive
        _recorded_values : Values as they are recorded in the PI Archive
        """
        return self.pi_point_list.Summary(
            time_range,
            summary_types,
            calculation_basis,
            time_type,
            self.page_config
        )

    def _filtered_summaries(self, time_range, interval, filter_expression, summary_types, calculation_basis,
                            filter_evaluation, filter_interval, time_type):
        """
        Get filtered calculation summary values from the archive within the specified ``time_range`` and at the
        specified ``interval``.  Returned results are provided at the specified ``interval`` by calculating the
        point summary over the ``interval``.

        Parameters
        ----------
        time_range : AF.Time.AFTimeRange
            Time range over which to query the PIPoint(s)
        interval : AF.Time.AFTimeSpan
            Interval at which to return values within the ''time_range''
        filter_expression : str
            OSISoft PI filter expression for returned recorded values
        summary_types : AF.Data.AFSummaryTypes
            Type of summary (e.g. 'average', 'max, 'min) to provide
        calculation_basis : AF.Data.AFCalculationBasis
            Calculation basis (e.g. 'TimeWeighted', 'EventWeighted')
        filter_evaluation : AF.Data.AFSampleType
        filter_interval : AF.Time.AFTimeSpan
        time_type : AF.Data.AFTimestampCalculation
            Timestamp (start or end) to provide

        Returns
        -------
        AF.Asset.AFValues object containing the response values

        See Also
        --------
        _current_value : Current values in the PI Snapshot table
        _plot_values : Values at regular intervals (pixels) for plotting
        _summaries : Data aggregations
        _interpolated_values : Values at regular intervals defined by TimeSpan
        _recorded_value : Last value recorded in the PI Archive
        _recorded_values : Values as they are recorded in the PI Archive
        """
        return self.pi_point_list.FilteredSummaries(time_range,
                                                    interval,
                                                    filter_expression,
                                                    summary_types,
                                                    calculation_basis,
                                                    filter_evaluation,
                                                    filter_interval,
                                                    time_type,
                                                    self.page_config)

    def __get_pi_point_attribute(self, attribute):
        self.__load_attributes()
        return {self.get_point_name(point): point.GetAttribute(attribute) for point in self.pi_point_list}

    def __get_pi_point_attributes(self, attributes: list = None):
        """

        Parameters
        ----------
        attributes : list, default None
            List of attributes to retrieve from the PIPointList
        """
        self.__load_attributes()
        if attributes is None:
            attributes = []
        return {self.get_point_name(point): {att.Key.lower(): att.Value for att in point.GetAttributes(attributes)}
                for point in self.pi_point_list}

    def get_point_name(self, point: AF.PI.PIPoint):
        if len(self.servers) > 1:
            return f"{point.Server.Name}\\{point.Name}"
        return point.Name

    def __load_attributes(self):
        """Load the raw attributes of the PI Point from the server"""
        if not self.__attributes_loaded and self.count > 0:
            self.pi_point_list.LoadAttributes()
            self.__attributes_loaded = True

    @staticmethod
    def from_fully_qualified_tags(tags):
        tags = [t.replace('/', '\\') for t in tags]

        tArray = Array[String](tags)
        o = AF.PI.PIPoint.FindPIPointsByPath(tArray)
        pi_point_list = AF.PI.PIPointList(o.Results.Values)
        return PIPointList(pi_point_list)

        # self.pi_point_list = pl_sdk
        # pl.pi_point_list = pl_sdk

        # for point in self.pi_point_list:
        #     self.__attributes[point.Name] = {
        #         att.Key.lower(): att.Value for att in point.GetAttributes([])
        #     }  # type: dict

    def _interpolated_value(self, time: AF.Time.AFTime) -> AF.Asset.AFValue:
        raise NotImplementedError

    def _update_value(
            self,
            value: AF.Asset.AFValue,
            update_mode: AF.Data.AFUpdateOption,
            buffer_mode: AF.Data.AFBufferOption,
    ) -> None:
        raise NotImplementedError

    def name(self) -> str:
        raise NotImplementedError

    def units_of_measurement(self) -> str | None:
        raise NotImplementedError


if __name__ == "__main__":
    pl = PIPointList()
    pl._find_pi_points_fully_qualified(['\\\\HST_HNG\\HNG04A_CT:L30D_B', '\\\\HST_MND\\HNG04_HNG4.V.IPE.RH0.A'])

    # print(pl.interpolated_values('1-1-2020', '1-2-2020', '6h'))
    print(pl.summaries('1-1-2024', '1-2-2024', '6h'))
