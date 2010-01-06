#!/usr/bin/env python

## NOTES
# Elevation gain or loss is not included in distance calculations

import subprocess
import sys
import tempfile
import thread
import threading
from datetime import datetime, timedelta

import genshi
import genshi.template
import gobject
import gst

import lxml.etree
import geolocator

PASSTHRU_FIELDS=set([
    'width',
    'height',
    'framerate',
    'pixel-aspect-ratio',
])

class Converter(object):
    def __init__(self, infile, outfile):
        self.player = gst.parse_launch(
            'filesrc name=video_in' +
            ' ! decodebin name=decoder' +
            ' oggmux name=mux' +
            ' ! filesink name=video_out' +
            ' queue name=q_audio_in' +
            ' ! audioconvert' +
            ' ! vorbisenc' +
            ' ! queue name=q_audio_out' +
            ' ! mux.' +
            ' queue name=q_orig_video_in' +
            ' ! ffmpegcolorspace' +
            ' ! pngenc compression-level=1 snapshot=false' +
            ' ! queue name=q_orig_video_out' +
            ' ! appsink name=png_sink' +
            ' appsrc name=png_src' + # TODO: capsset image/svg+xml ! rsvgdec; for now, output is png; URGENT: set framerate, res, etc.
            ' ! queue name=q_new_video_in' +
            ' ! pngdec' +
            ' ! ffmpegcolorspace' +
            ' ! theoraenc' +
            ' ! queue name=q_new_video_out' +
            ' ! mux.'
        )
        self.player.set_state(gst.STATE_NULL)
        self.player.get_by_name('video_in').set_property('location', infile)
        self.player.get_by_name('video_out').set_property('location', outfile)

        self.decoder = self.player.get_by_name('decoder')
        self.decoder.connect('new-decoded-pad', self.on_new_decoded_pad)

        self.png_sink = self.player.get_by_name('png_sink')
        self.png_sink.set_property('drop', False)
        self.png_sink.set_property('max_buffers', 1)

        self.png_src = self.player.get_by_name('png_src')

        player_bus = self.player.get_bus()
        player_bus.add_signal_watch()
        player_bus.connect('message', self.on_message)

        self.app_caps = False
        self.ready_lock = threading.Lock()
        self.ready_lock.acquire()

        self.time_format = gst.Format(gst.FORMAT_TIME)

        self.infile = infile
    def make_app_caps(self, old_caps):
        caps = gst.Caps(old_caps.to_string())
        caps[0].set_name('image/png')
        for key in list(old_caps[0].keys()):
            if not key in PASSTHRU_FIELDS:
                caps[0].remove_field(key)
        return caps
    def run(self, loop):
        print ' --- Attempting to set state to PAUSED'
        self.player.set_state(gst.STATE_PAUSED)
        # TODO: need to wait for the magic to happen before proceeding?
        print ' --- Waiting for ready lock'
        self.ready_lock.acquire()
        print ' --- Attempting to set state to PLAYING'
        self.png_sink.set_state(gst.STATE_PLAYING)
        self.player.set_state(gst.STATE_PLAYING)
        frameno = 1
        while True:
            buf = self.png_sink.emit('pull_buffer')
            if buf == None:
                self.png_src.emit('end_of_stream')
                break
            data = self.get_data()
            print ' --- Processing frame {0} ({1:.2f}%)'.format(frameno, (float(data['stream_position']) / float(data['stream_duration'])) * 100)
            buf = self.filter_buffer(buf, **data)
            self.png_src.emit('push_buffer', buf)
            frameno += 1
        loop.quit()
    def get_data(self):
        retval = {}
        retval['stream_position'] = self.player.query_position(self.time_format, None)[0]
        retval['stream_duration'] = self.player.query_duration(self.time_format, None)[0]
        retval['width'] = str(self.app_caps[0]['width'])
        retval['height'] = str(self.app_caps[0]['height'])
        return retval
    def filter_buffer(self, buffer_in, **kwargs):
        # TODO: actually write this
        return buffer_in
    def on_new_decoded_pad(self, dbin, pad, is_last):
        caps = pad.get_caps()
        caps_str = caps.to_string()
        if 'audio' in caps_str:
            print '--- got audio caps: %s' % caps_str
            pad.link(self.player.get_by_name('q_audio_in').get_pad('sink'))
        if 'video' in caps_str:
            print '--- got video caps: %s' % caps_str
            qsink = self.player.get_by_name('q_orig_video_in').get_pad('sink')
            pad.link(qsink)
            qsink.connect('notify::caps', self.do_notify_caps)
    def do_notify_caps(self, pad, args):
        caps = pad.get_negotiated_caps()
        if not caps: return
        # update output caps
        self.app_caps = self.make_app_caps(caps)
        self.png_src.set_property('caps', self.app_caps)
        print ' --- APP CAPS: %s' % (self.app_caps,)
        self.ready_lock.release()
    def on_message(self, bus, message):
        self._state_change_detected = True
        t = message.type
        if t == gst.MESSAGE_STATE_CHANGED:
            print 'state changed: %r' % (message,)
        elif t == gst.MESSAGE_ERROR:
            err, debug = message.parse_error()
            print ' --- Error: %s' % ((err, debug),)
            self.player.set_state(gst.STATE_NULL)
        else:
            print 'Unrecognized message: %r' % (message,)

class SVGConverter(Converter):
    def __init__(self, video_in, video_out, svg_template_text):
        Converter.__init__(self, video_in, video_out)
        self.svg_template = genshi.template.MarkupTemplate(svg_template_text)
        self.tempfile_svg = tempfile.NamedTemporaryFile(suffix='.svg')
        self.tempfile_png_in = tempfile.NamedTemporaryFile(suffix='.in.png')
        self.tempfile_png_out = tempfile.NamedTemporaryFile(suffix='.out.png')
    def get_data_for_time(self, video_time_ns):
        return {}
    def filter_buffer(self, buffer_in, **kwargs):
        # TODO: lock?
        kwargs['input_frame_filename'] = self.tempfile_png_in.name
        kwargs.update(self.get_data_for_time(kwargs['stream_position']))

        self.tempfile_png_in.seek(0)
        self.tempfile_png_in.truncate()
        self.tempfile_png_in.write(buffer_in)
        self.tempfile_png_in.flush()

        self.tempfile_svg.seek(0)
        self.tempfile_svg.truncate()
        self.tempfile_svg.write(self.svg_template.generate(**kwargs).render())
        self.tempfile_svg.flush()

        subprocess.check_call([
            'rsvg',
            '-w', kwargs['width'],
            '-h', kwargs['height'],
            self.tempfile_svg.name,
            self.tempfile_png_out.name,
        ])

        self.tempfile_png_out.seek(0)
        buffer_out = gst.Buffer(self.tempfile_png_out.read())
        buffer_out.caps = buffer_in.caps
        buffer_out.timestamp = buffer_in.timestamp
        buffer_out.duration = buffer_in.duration
        return buffer_out

# YYYY-MM-DDTHH:MM:SS
TIME_FMT_STR='%Y-%m-%dT%H:%M:%S'
GPX_NS='http://www.topografix.com/GPX/1/1'
GPX='{%s}' % GPX_NS

def timedelta_to_seconds(td):
    return (
        td.days * 24 * 3600 +
        td.seconds +
        td.microseconds * 0.000001
    )

class GPXConverter(SVGConverter):
    def __init__(self, video_in, video_out, svg_template_text, gpx_file_name, video_start_time):
        SVGConverter.__init__(self, video_in, video_out, svg_template_text)
        if isinstance(video_start_time, basestring):
            video_start_time = datetime.strptime(video_start_time, TIME_FMT_STR)
        self.start_time = video_start_time
        self.gpx_data = lxml.etree.parse(open(gpx_file_name))
        # set up state
        self.gpx_data_iter = self.get_trackpoint_iterator()
        self.prev_point = self.gpx_data_iter.next()
        self.next_point = self.gpx_data_iter.next()
        self.calc_curr_dist()
        self.prior_dist = 0.0 # distance previously traveled
        self.climb = 0.0
        self.descent = 0.0
        self.finished = False # if data stream ended
    def get_trackpoint_iterator(self):
        for trkseg in lxml.etree.ETXPath('//{GPX}trkseg'.format(GPX=GPX))(self.gpx_data):
            for trkpt in trkseg.getchildren():
                time_str = trkpt.find('{GPX}time'.format(GPX=GPX)).text.rstrip('Z')
                time_obj = datetime.strptime(time_str, TIME_FMT_STR)
                data = {
                    'lat': float(trkpt.attrib['lat']),
                    'lon': float(trkpt.attrib['lon']),
                    'ele': float(trkpt.find('{GPX}ele'.format(GPX=GPX)).text),
                    'time': time_obj
                }
                yield data
    def calc_curr_dist(self):
        self.elevation_delta = self.next_point['ele'] - self.prev_point['ele']
        self.curr_dist = geolocator.gislib.getDistance(
            (self.prev_point['lon'], self.prev_point['lat']),
            (self.next_point['lon'], self.next_point['lat']),
        ) * (geolocator.gislib.kmsPerNauticalMile * 1000)
    def get_data_for_time(self, video_time_ns):
        """
        Input:  time in nanoseconds since start of video playback
        Output: elevation, distance traveled, current speed
        """
        video_time_delta = timedelta(microseconds=video_time_ns / 1000)
        video_time = self.start_time + video_time_delta
        if self.finished or video_time < self.prev_point['time']:
            return { 'data_available': False, }
        while video_time > self.next_point['time']:
            # determine span-global data
            if self.elevation_delta > 0:
                self.climb += self.elevation_delta
            else:
                self.descent -= self.elevation_delta
            self.prev_point = self.next_point
            try:
                self.next_point = self.gpx_data_iter.next()
            except StopIteration:
                self.finished = True
                return { 'data_available': False, }
            self.prior_dist += self.curr_dist
            self.calc_curr_dist()
            self.curr_span = self.next_point['time'] - self.prev_point['time']
            self.curr_span_seconds = timedelta_to_seconds(self.curr_span)
            self.curr_speed = self.curr_dist / self.curr_span_seconds
            # determine prev, next point times as seconds from start of video
            self.prev_point_seconds = timedelta_to_seconds(self.prev_point['time'] - self.start_time)
            self.next_point_seconds = timedelta_to_seconds(self.next_point['time'] - self.start_time)
        # determine frame-local data
        percent_span_completion = (
            (timedelta_to_seconds(video_time_delta) - self.prev_point_seconds)
            / self.curr_span_seconds
        )
        return {
            'data_available': True,
            'time': video_time,
            'speed': self.curr_speed,
            'elevation': self.prev_point['ele'] +
                (self.elevation_delta * percent_span_completion),
            'dist': self.prior_dist +
                (self.curr_dist * percent_span_completion),
        }

def main():
    gobject.threads_init()
    loop = gobject.MainLoop()
    c = GPXConverter(sys.argv[1], sys.argv[2], open(sys.argv[3]).read(), sys.argv[4], sys.argv[5])
    thread.start_new_thread(c.run, (loop,))
    loop.run()

if __name__ == '__main__':
    main()

# vim: ai et sw=4 sts=4 ts=4
