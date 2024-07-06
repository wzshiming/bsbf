package bsbf

import (
	"reflect"
	"testing"
)

func Test_seekLine(t *testing.T) {
	data := []byte(`ab
cd
ef`)
	type args struct {
		s       []byte
		off     int64
		bufSize int64
	}
	tests := []struct {
		name     string
		args     args
		want     Range
		wantData []byte
		wantErr  bool
	}{
		{
			args: args{
				s:       data,
				off:     0,
				bufSize: 1,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     1,
				bufSize: 1,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     2,
				bufSize: 1,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     3,
				bufSize: 1,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     4,
				bufSize: 1,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     5,
				bufSize: 1,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     6,
				bufSize: 1,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},
		{
			args: args{
				s:       data,
				off:     7,
				bufSize: 1,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},
		{
			args: args{
				s:       data,
				off:     8,
				bufSize: 1,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},

		{
			args: args{
				s:       data,
				off:     0,
				bufSize: 2,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     1,
				bufSize: 2,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     2,
				bufSize: 2,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     3,
				bufSize: 2,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     4,
				bufSize: 2,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     5,
				bufSize: 2,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     6,
				bufSize: 2,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},
		{
			args: args{
				s:       data,
				off:     7,
				bufSize: 2,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},
		{
			args: args{
				s:       data,
				off:     8,
				bufSize: 2,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},

		{
			args: args{
				s:       data,
				off:     0,
				bufSize: 3,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     1,
				bufSize: 3,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     2,
				bufSize: 3,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     3,
				bufSize: 3,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     4,
				bufSize: 3,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     5,
				bufSize: 3,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     6,
				bufSize: 3,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},
		{
			args: args{
				s:       data,
				off:     7,
				bufSize: 3,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},
		{
			args: args{
				s:       data,
				off:     8,
				bufSize: 3,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},

		{
			args: args{
				s:       data,
				off:     0,
				bufSize: 4,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     1,
				bufSize: 4,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     2,
				bufSize: 4,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     3,
				bufSize: 4,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     4,
				bufSize: 4,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     5,
				bufSize: 4,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     6,
				bufSize: 4,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},
		{
			args: args{
				s:       data,
				off:     7,
				bufSize: 4,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},
		{
			args: args{
				s:       data,
				off:     8,
				bufSize: 4,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},

		{
			args: args{
				s:       data,
				off:     0,
				bufSize: 5,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     1,
				bufSize: 5,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     2,
				bufSize: 5,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     3,
				bufSize: 5,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     4,
				bufSize: 5,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     5,
				bufSize: 5,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     6,
				bufSize: 5,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},
		{
			args: args{
				s:       data,
				off:     7,
				bufSize: 5,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},
		{
			args: args{
				s:       data,
				off:     8,
				bufSize: 5,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},

		{
			args: args{
				s:       data,
				off:     0,
				bufSize: 6,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     1,
				bufSize: 6,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     2,
				bufSize: 6,
			},
			want: Range{
				Begin: 0,
				End:   3,
			},
			wantData: []byte("ab"),
		},
		{
			args: args{
				s:       data,
				off:     3,
				bufSize: 6,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     4,
				bufSize: 6,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     5,
				bufSize: 6,
			},
			want: Range{
				Begin: 3,
				End:   6,
			},
			wantData: []byte("cd"),
		},
		{
			args: args{
				s:       data,
				off:     6,
				bufSize: 6,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},
		{
			args: args{
				s:       data,
				off:     7,
				bufSize: 6,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},
		{
			args: args{
				s:       data,
				off:     8,
				bufSize: 6,
			},
			want: Range{
				Begin: 6,
				End:   8,
			},
			wantData: []byte("ef"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotData, err := seekLine(mmap(tt.args.s), int64(len(tt.args.s)), tt.args.bufSize, lineSep, tt.args.off)
			if (err != nil) != tt.wantErr {
				t.Errorf("seekLine() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("seekLine() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(gotData, tt.wantData) {
				t.Errorf("seekLine() gotData = %v, want %v", gotData, tt.wantData)
			}
		})
	}
}
